#!/usr/bin/env python3
"""Bounded, fail-closed YouTube worker. Default invocation is entirely offline.

Never logs credentials, response bodies, or resumable session URLs. A lost
mutation response is reconciled by a read before any further mutation. There
are deliberately no automatic HTTP retries and no delete operation.
"""
from __future__ import annotations

import argparse
import base64
import copy
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

CHANNEL_ID = "UCqHmPyl4dRExH1pTBeMuqBA"
REPOSITORY = "bally65/course-learning-videos"
SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
API_BASE = "https://www.googleapis.com/youtube/v3/"
UPLOAD_BASE = "https://www.googleapis.com/upload/youtube/v3/"
STATE_PATH = "cloud-state.json"
MAX_JOBS = 5
MAX_SECONDS = 90 * 60
MAX_CAPTION_BYTES = 100 * 1024 * 1024
QUOTA_REASONS = {"quotaExceeded", "dailyLimitExceeded", "uploadLimitExceeded"}
PHASES = {"pending", "upload_initiated", "upload_unknown", "uploaded",
          "caption_initiated", "caption_attached", "processing",
          "public_update_initiated", "public_api_confirmed", "needs_review"}


class Stop(Exception):
    """Only fixed, non-sensitive error messages may be passed to this class."""


class ValidationError(Stop):
    pass


class StateWriteError(Stop):
    pass


class UnknownResult(Stop):
    pass


class RunDeadline(Stop):
    pass


class APIError(Stop):
    def __init__(self, reason="api_error", status=0, rejected_operation=None):
        self.reason = reason if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", reason) else "api_error"
        self.status = status
        self.rejected_operation = rejected_operation
        super().__init__(self.reason)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def valid_video_id(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", value))


def validate_asset_url(url):
    if not isinstance(url, str):
        raise ValidationError("asset_url_invalid")
    parsed = urlsplit(url)
    prefix = "/" + REPOSITORY + "/releases/download/"
    tail = parsed.path[len(prefix):] if parsed.path.startswith(prefix) else ""
    if (parsed.scheme != "https" or parsed.netloc != "github.com" or
            parsed.query or parsed.fragment or not tail or len(tail.split("/")) != 2 or
            any(x in tail for x in ("..", "%", "\\")) or not all(tail.split("/"))):
        raise ValidationError("asset_url_outside_fixed_release_repository")


def validate_queue(queue):
    if not isinstance(queue, dict) or queue.get("schema_version") != 1:
        raise ValidationError("queue_schema_invalid")
    if queue.get("channel_id") != CHANNEL_ID or queue.get("repository") != REPOSITORY:
        raise ValidationError("queue_target_invalid")
    if type(queue.get("enabled")) is not bool:
        raise ValidationError("queue_enabled_must_be_boolean")
    jobs = queue.get("jobs")
    if not isinstance(jobs, list):
        raise ValidationError("queue_jobs_invalid")
    excluded = queue.get("initial_published_job_ids", [])
    if not isinstance(excluded, list) or not all(isinstance(x, str) for x in excluded):
        raise ValidationError("queue_exclusions_invalid")
    seen = set()
    for job in jobs:
        if not isinstance(job, dict):
            raise ValidationError("job_invalid")
        jid = job.get("id")
        if (not isinstance(jid, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", jid)
                or jid in seen or jid in excluded):
            raise ValidationError("job_id_duplicate_invalid_or_already_published")
        seen.add(jid)
        for field, limit in (("title", 100), ("description", 4900)):
            value = job.get(field)
            if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
                raise ValidationError("job_metadata_invalid")
        for field in ("language", "caption_language"):
            if not isinstance(job.get(field), str) or not re.fullmatch(r"[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*", job[field]):
                raise ValidationError("job_language_invalid")
        if not isinstance(job.get("category_id"), str) or not job["category_id"].isdigit():
            raise ValidationError("job_category_invalid")
        for field in ("sha256", "srt_sha256"):
            if not isinstance(job.get(field), str) or not re.fullmatch(r"[a-f0-9]{64}", job[field]):
                raise ValidationError("job_hash_invalid")
        if type(job.get("bytes")) is not int or job["bytes"] <= 0:
            raise ValidationError("job_bytes_invalid")
        if "srt_bytes" in job and (type(job["srt_bytes"]) is not int or not 0 < job["srt_bytes"] <= MAX_CAPTION_BYTES):
            raise ValidationError("job_srt_bytes_invalid")
        for field in ("made_for_kids", "contains_synthetic_media"):
            if field in job and type(job[field]) is not bool:
                raise ValidationError("job_status_metadata_invalid")
        if "license" in job and job["license"] not in {"youtube", "creativeCommon"}:
            raise ValidationError("job_license_invalid")
        if job.get("content_reviewed") is not True or job.get("uncertainties_preserved") is not True:
            raise ValidationError("job_content_review_not_confirmed")
        if not isinstance(job.get("uncertainties"), list) or not all(isinstance(x, str) for x in job["uncertainties"]):
            raise ValidationError("job_uncertainties_invalid")
        for field in ("video_url", "srt_url"):
            validate_asset_url(job.get(field))
    return queue


def queue_fingerprint(queue):
    # Enabling a reviewed queue does not invalidate its bootstrap state.
    return digest({"channel_id": CHANNEL_ID, "repository": REPOSITORY,
                   "jobs": queue["jobs"],
                   "initial_published_job_ids": queue.get("initial_published_job_ids", [])})


def stable_tag(job):
    return "course-cloud-" + hashlib.sha256((job["id"] + ":" + job["sha256"]).encode()).hexdigest()[:40]


def caption_name(job):
    return "source-" + job["srt_sha256"] + "-" + job["caption_language"]


def empty_state(queue):
    validate_queue(queue)
    return {"schema_version": 1, "channel_id": CHANNEL_ID, "repository": REPOSITORY,
            "queue_sha256": queue_fingerprint(queue), "all_done": False,
            "jobs": {job["id"]: {"fingerprint": digest(job), "stable_tag": stable_tag(job),
                                   "phase": "pending"} for job in queue["jobs"]}}


def validate_state(state, queue):
    if (not isinstance(state, dict) or state.get("schema_version") != 1 or
            state.get("channel_id") != CHANNEL_ID or state.get("repository") != REPOSITORY or
            state.get("queue_sha256") != queue_fingerprint(queue) or
            type(state.get("all_done")) is not bool or not isinstance(state.get("jobs"), dict)):
        raise ValidationError("state_queue_binding_invalid")
    if set(state["jobs"]) != {job["id"] for job in queue["jobs"]}:
        raise ValidationError("state_job_set_invalid")
    for job in queue["jobs"]:
        entry = state["jobs"][job["id"]]
        if (not isinstance(entry, dict) or entry.get("fingerprint") != digest(job) or entry.get("stable_tag") != stable_tag(job)
                or entry.get("phase") not in PHASES):
            raise ValidationError("state_job_binding_invalid")
        if "video_id" in entry and not valid_video_id(entry["video_id"]):
            raise ValidationError("state_video_id_invalid")
        if entry["phase"] == "public_api_confirmed" and not valid_video_id(entry.get("video_id")):
            raise ValidationError("state_completion_invalid")
    if state["all_done"] and not all(x["phase"] == "public_api_confirmed" for x in state["jobs"].values()):
        raise ValidationError("state_all_done_invalid")
    # No secret-bearing keys or URLs may enter a committed state.
    serialized = canonical(state).decode()
    if re.search(r"(?i)refresh_token|access_token|client_secret|authorization|upload_id=|session_url", serialized):
        raise ValidationError("state_contains_forbidden_secret_fields")
    return state


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTP:
    """One request per call, no retries; never expose a response body in errors."""
    def __init__(self, deadline=None):
        self.opener = build_opener(NoRedirect())
        self.deadline = deadline

    def check_time(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise RunDeadline("run_time_limit")

    def request(self, method, url, headers=None, data=None, accepted=(200, 201), asset=False):
        self.check_time()
        if method not in {"GET", "POST", "PUT"}:
            raise ValidationError("http_method_not_allowed")
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443):
            raise ValidationError("request_url_invalid")
        hosts = {"api.github.com", "www.googleapis.com", "oauth2.googleapis.com"}
        if asset:
            hosts = {"github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"}
        if parsed.hostname not in hosts:
            raise ValidationError("request_host_not_allowed")
        timeout = 60
        if self.deadline is not None:
            timeout = max(1, min(timeout, self.deadline - time.monotonic()))
        try:
            response = self.opener.open(Request(url, data=data, headers=headers or {}, method=method), timeout=timeout)
        except HTTPError as error:
            if error.code in accepted:
                return error
            # An HTTP failure body can contain a session URL or auth details.
            try:
                payload = json.loads(error.read(65536))
                reason = payload.get("error", {}).get("errors", [{}])[0].get("reason", "http_error")
            except (ValueError, AttributeError, IndexError, TypeError):
                reason = "http_error"
            finally:
                error.close()
            if error.code >= 500 and method != "GET":
                raise UnknownResult("mutation_outcome_unknown") from None
            rejected = None
            if 400 <= error.code < 500 and reason in QUOTA_REASONS and parsed.netloc == "www.googleapis.com":
                if method in {"POST", "PUT"} and parsed.path == "/upload/youtube/v3/videos":
                    rejected = "upload"
                elif method == "POST" and parsed.path == "/upload/youtube/v3/captions":
                    rejected = "caption"
                elif method == "PUT" and parsed.path == "/youtube/v3/videos":
                    rejected = "public"
            raise APIError(reason, error.code, rejected_operation=rejected) from None
        except (URLError, TimeoutError, OSError, HTTPException):
            raise UnknownResult("network_outcome_unknown") from None
        if response.status not in accepted:
            response.close()
            raise APIError("unexpected_response", response.status)
        return response

    def json(self, method, url, headers=None, value=None, data=None, accepted=(200, 201)):
        hdr = dict(headers or {})
        if value is not None:
            data = canonical(value)
            hdr["Content-Type"] = "application/json; charset=UTF-8"
        with self.request(method, url, hdr, data, accepted) as response:
            try:
                return json.loads(response.read(2 * 1024 * 1024))
            except (ValueError, OSError, HTTPException):
                raise UnknownResult("response_not_decodable") from None


class GitHubStore:
    def __init__(self, http, token, branch="main"):
        if not re.fullmatch(r"[A-Za-z0-9_./-]{1,200}", branch):
            raise ValidationError("branch_invalid")
        self.http, self.branch, self.sha = http, branch, None
        self.url = "https://api.github.com/repos/" + REPOSITORY + "/contents/" + STATE_PATH
        self.headers = {"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "course-cloud-worker"}

    def load(self):
        result = self.http.json("GET", self.url + "?" + urlencode({"ref": self.branch}), self.headers)
        try:
            if result.get("encoding") != "base64" or not re.fullmatch(r"[a-f0-9]{40,64}", result["sha"]):
                raise ValueError
            self.sha = result["sha"]
            return json.loads(base64.b64decode(result["content"], validate=False))
        except (KeyError, ValueError, TypeError):
            raise ValidationError("remote_state_invalid_or_missing") from None

    def commit(self, state):
        if self.sha is None:
            raise StateWriteError("state_not_loaded")
        body = {"message": "Persist bounded uploader state [skip ci]", "branch": self.branch,
                "sha": self.sha, "content": base64.b64encode(canonical(state) + b"\n").decode()}
        try:
            result = self.http.json("PUT", self.url, self.headers, value=body)
            new_sha = result["content"]["sha"]
            if not re.fullmatch(r"[a-f0-9]{40,64}", new_sha):
                raise ValueError
            self.sha = new_sha
        except (Stop, KeyError, TypeError, ValueError):
            # Never retry an unknown commit or merge over a concurrent writer.
            raise StateWriteError("state_commit_failed_stop_before_next_external_step") from None


class ReleaseAssets:
    def __init__(self, http):
        self.http = http

    def fetch(self, job, directory, caption_only=False):
        paths = []
        sources = [("srt_url", "srt_sha256", job.get("srt_bytes"), ".srt")]
        if not caption_only:
            sources.insert(0, ("video_url", "sha256", job["bytes"], ".mp4"))
        for key, hashkey, expected_size, suffix in sources:
            url = job[key]
            validate_asset_url(url)
            response = None
            for _ in range(4):
                response = self.http.request("GET", url, accepted=(200, 301, 302, 303, 307, 308), asset=True)
                if response.status == 200:
                    break
                target = response.headers.get("Location", "")
                response.close()
                parsed = urlsplit(target)
                if parsed.scheme != "https" or parsed.hostname not in {"release-assets.githubusercontent.com", "objects.githubusercontent.com"}:
                    raise ValidationError("asset_redirect_invalid")
                url = target
            else:
                raise ValidationError("asset_redirect_limit")
            path = Path(directory) / (job["id"] + suffix)
            count, hashed = 0, hashlib.sha256()
            limit = expected_size if expected_size is not None else MAX_CAPTION_BYTES
            with response, path.open("wb") as target:
                while True:
                    self.http.check_time()
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > limit:
                        raise ValidationError("asset_size_exceeds_manifest")
                    hashed.update(chunk)
                    target.write(chunk)
            if (expected_size is not None and count != expected_size) or count == 0 or hashed.hexdigest() != job[hashkey]:
                raise ValidationError("asset_hash_or_size_mismatch")
            paths.append(path)
        return tuple(paths)


class YouTube:
    def __init__(self, http, client_id, client_secret, refresh_token):
        self.http = http
        self.oauth = {"client_id": client_id, "client_secret": client_secret,
                      "refresh_token": refresh_token, "grant_type": "refresh_token"}
        self.token, self.expires_at, self.uploads_playlist = None, 0, None

    def headers(self):
        if self.token is None or time.monotonic() >= self.expires_at:
            data = urlencode(self.oauth).encode()
            result = self.http.json("POST", "https://oauth2.googleapis.com/token",
                                    {"Content-Type": "application/x-www-form-urlencoded"}, data=data)
            if result.get("scope") and set(result["scope"].split()) != {SCOPE}:
                raise ValidationError("oauth_scope_must_be_only_youtube_force_ssl")
            if not isinstance(result.get("access_token"), str) or not result["access_token"]:
                raise ValidationError("oauth_refresh_failed")
            self.token = result["access_token"]
            self.expires_at = time.monotonic() + max(1, int(result.get("expires_in", 3600)) - 120)
        return {"Authorization": "Bearer " + self.token}

    def call(self, method, resource, params, value=None):
        return self.http.json(method, API_BASE + resource + "?" + urlencode(params), self.headers(), value=value)

    def verify_channel(self):
        result = self.call("GET", "channels", {"part": "id,contentDetails", "mine": "true", "maxResults": 50})
        items = result.get("items", [])
        if len(items) != 1 or items[0].get("id") != CHANNEL_ID or result.get("nextPageToken"):
            raise ValidationError("oauth_channel_does_not_uniquely_match_target")
        self.uploads_playlist = items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
        if not self.uploads_playlist:
            raise ValidationError("channel_uploads_playlist_missing")

    def get_video(self, video_id):
        result = self.call("GET", "videos", {"part": "id,snippet,status,processingDetails", "id": video_id})
        items = result.get("items", [])
        if len(items) != 1 or items[0].get("id") != video_id or items[0].get("snippet", {}).get("channelId") != CHANNEL_ID:
            raise ValidationError("video_not_uniquely_owned_by_target")
        return items[0]

    def reconcile_upload(self, tag, max_pages=5):
        matches, page = set(), None
        for _ in range(max_pages):
            params = {"part": "contentDetails", "playlistId": self.uploads_playlist, "maxResults": 50}
            if page:
                params["pageToken"] = page
            result = self.call("GET", "playlistItems", params)
            ids = [x.get("contentDetails", {}).get("videoId") for x in result.get("items", [])]
            if any(not valid_video_id(x) for x in ids):
                raise ValidationError("uploads_playlist_response_invalid")
            if ids:
                videos = self.call("GET", "videos", {"part": "id,snippet", "id": ",".join(ids)})
                for video in videos.get("items", []):
                    snippet = video.get("snippet", {})
                    if snippet.get("channelId") == CHANNEL_ID and tag in snippet.get("tags", []):
                        matches.add(video["id"])
            page = result.get("nextPageToken")
            if not page:
                return sorted(matches)
        # A truncated scan cannot establish uniqueness. Never guess or re-upload.
        return None

    def upload_private(self, job, video_path, tag):
        body = {"snippet": {"title": job["title"], "description": job["description"],
                            "categoryId": job["category_id"], "defaultLanguage": job["language"],
                            "defaultAudioLanguage": job["language"], "tags": [tag]},
                "status": {"privacyStatus": "private"}}
        for source, target in (("made_for_kids", "selfDeclaredMadeForKids"),
                               ("contains_synthetic_media", "containsSyntheticMedia"), ("license", "license")):
            if source in job:
                body["status"][target] = job[source]
        headers = self.headers()
        headers.update({"Content-Type": "application/json; charset=UTF-8",
                        "X-Upload-Content-Type": "video/mp4", "X-Upload-Content-Length": str(job["bytes"])})
        url = UPLOAD_BASE + "videos?" + urlencode({"uploadType": "resumable", "part": "snippet,status"})
        with self.http.request("POST", url, headers, canonical(body)) as response:
            location = response.headers.get("Location", "")
        parsed = urlsplit(location)
        if (parsed.scheme != "https" or parsed.netloc != "www.googleapis.com" or
                parsed.path != "/upload/youtube/v3/videos" or parsed.fragment):
            raise ValidationError("resumable_location_invalid")
        # Session URI lives only in this stack frame. A process restart reconciles
        # via the stable video tag, never by serializing this credential-like URI.
        headers = self.headers()
        headers.update({"Content-Type": "video/mp4", "Content-Length": str(job["bytes"])})
        with video_path.open("rb") as stream:
            result = self.http.json("PUT", location, headers, data=stream)
        if not valid_video_id(result.get("id")):
            raise UnknownResult("upload_video_id_missing")
        return result["id"]

    def list_captions(self, video_id):
        result = self.call("GET", "captions", {"part": "id,snippet", "videoId": video_id})
        return result.get("items", [])

    def insert_caption(self, job, video_id, srt_path):
        boundary = "course_caption_" + job["srt_sha256"][:32]
        metadata = {"snippet": {"videoId": video_id, "language": job["caption_language"],
                                "name": caption_name(job), "isDraft": False}}
        data = (b"--" + boundary.encode() + b"\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n" +
                canonical(metadata) + b"\r\n--" + boundary.encode() +
                b"\r\nContent-Type: application/octet-stream\r\n\r\n" + srt_path.read_bytes() +
                b"\r\n--" + boundary.encode() + b"--\r\n")
        headers = self.headers()
        headers["Content-Type"] = "multipart/related; boundary=" + boundary
        result = self.http.json("POST", UPLOAD_BASE + "captions?uploadType=multipart&part=snippet", headers, data=data)
        if not isinstance(result.get("id"), str) or not result["id"]:
            raise UnknownResult("caption_id_missing")
        return result["id"]

    def set_public(self, video):
        allowed = {"embeddable", "license", "publicStatsViewable", "selfDeclaredMadeForKids", "containsSyntheticMedia"}
        status = {key: value for key, value in video.get("status", {}).items() if key in allowed}
        status["privacyStatus"] = "public"
        return self.call("PUT", "videos", {"part": "status"}, {"id": video["id"], "status": status})


class Runner:
    def __init__(self, queue, state, store, youtube, assets, max_jobs=MAX_JOBS, max_seconds=MAX_SECONDS, clock=time.monotonic):
        self.queue = validate_queue(queue)
        self.state = copy.deepcopy(validate_state(state, queue))
        self.store, self.youtube, self.assets = store, youtube, assets
        self.max_jobs = min(MAX_JOBS, max(1, max_jobs))
        self.clock, self.deadline = clock, clock() + min(MAX_SECONDS, max(1, max_seconds))

    def check_time(self):
        if self.clock() >= self.deadline:
            raise RunDeadline("run_time_limit")

    def commit(self, job=None, **updates):
        candidate = copy.deepcopy(self.state)
        if job is None:
            candidate.update(updates)
        else:
            candidate["jobs"][job["id"]].update(updates)
            candidate["jobs"][job["id"]]["updated_at"] = timestamp()
        candidate["updated_at"] = timestamp()
        validate_state(candidate, self.queue)
        self.store.commit(candidate)  # Must succeed before any next API call.
        self.state = candidate

    def needs_review(self, job, reason):
        self.commit(job, phase="needs_review", review_reason=reason)
        return "needs_review"

    def matching_captions(self, job, video_id):
        return [x for x in self.youtube.list_captions(video_id)
                if x.get("snippet", {}).get("name") == caption_name(job)
                and x.get("snippet", {}).get("language") == job["caption_language"]]

    def process(self, job, directory):
        entry = self.state["jobs"][job["id"]]
        if entry["phase"] == "needs_review":
            return "needs_review"
        video_id = entry.get("video_id")
        if not video_id and entry["phase"] in {"upload_initiated", "upload_unknown"}:
            matches = self.youtube.reconcile_upload(stable_tag(job))
            if matches is None or len(matches) != 1:
                return self.needs_review(job, "upload_reconciliation_not_unique")
            video_id = matches[0]
            if not valid_video_id(video_id):
                return self.needs_review(job, "reconciled_id_invalid")
            self.commit(job, phase="uploaded", video_id=video_id, reconciled=True)
        paths = None
        if not video_id:
            if entry["phase"] != "pending":
                return self.needs_review(job, "missing_video_id_in_nonpending_phase")
            paths = self.assets.fetch(job, directory)
            self.check_time()
            self.commit(job, phase="upload_initiated", upload_initiated_at=timestamp())
            video_id = self.youtube.upload_private(job, paths[0], stable_tag(job))
            if not valid_video_id(video_id):
                raise UnknownResult("upload_video_id_missing")
            self.commit(job, phase="uploaded", video_id=video_id)
        self.check_time()
        video = self.youtube.get_video(video_id)
        if stable_tag(job) not in video.get("snippet", {}).get("tags", []):
            return self.needs_review(job, "video_stable_tag_missing")
        captions = self.matching_captions(job, video_id)
        if len(captions) > 1:
            return self.needs_review(job, "caption_reconciliation_not_unique")
        entry = self.state["jobs"][job["id"]]
        public_update_attempted = bool(entry.get("public_update_attempted")) or entry["phase"] == "public_update_initiated"
        if not captions:
            if entry.get("caption_id") or entry["phase"] in {"caption_initiated", "caption_attached", "processing", "public_update_initiated"}:
                return self.needs_review(job, "caption_mutation_outcome_unknown_or_track_missing")
            srt_path = paths[1] if paths else self.assets.fetch(job, directory, caption_only=True)[0]
            self.check_time()
            self.commit(job, phase="caption_initiated", caption_name=caption_name(job))
            caption_id = self.youtube.insert_caption(job, video_id, srt_path)
            self.commit(job, phase="caption_attached", caption_id=caption_id)
            captions = self.matching_captions(job, video_id)
        if len(captions) != 1:
            return self.needs_review(job, "source_caption_not_uniquely_present")
        caption = captions[0]
        snippet = caption.get("snippet", {})
        if snippet.get("status") == "failed":
            return self.needs_review(job, "caption_processing_failed")
        processing = video.get("processingDetails", {}).get("processingStatus")
        upload_status = video.get("status", {}).get("uploadStatus")
        if processing in {"failed", "terminated"} or upload_status in {"failed", "rejected", "deleted"}:
            return self.needs_review(job, "video_processing_failed")
        if processing != "succeeded" or snippet.get("status") != "serving" or snippet.get("isDraft") is not False:
            self.commit(job, phase="processing", caption_id=caption["id"],
                        public_update_attempted=public_update_attempted)
            return "processing"  # No sleep/poll loop. A later bounded run checks again.
        self.check_time()
        if video.get("status", {}).get("privacyStatus") != "public":
            # An interrupted public update must be read back, never blindly replayed.
            if public_update_attempted:
                return self.needs_review(job, "public_update_outcome_not_confirmed")
            self.commit(job, phase="public_update_initiated", caption_id=caption["id"], public_update_attempted=True)
            self.youtube.set_public(video)
        confirmed = self.youtube.get_video(video_id)
        if confirmed.get("status", {}).get("privacyStatus") != "public":
            return self.needs_review(job, "public_status_not_confirmed")
        self.commit(job, phase="public_api_confirmed", caption_id=caption["id"],
                    public_api_confirmed_at=timestamp(), playback_verification="not_performed")
        return "public_api_confirmed"

    def run(self):
        if not self.queue["enabled"]:
            raise ValidationError("queue_disabled")
        if self.state["all_done"]:
            return "all_done_noop"
        if any(x["phase"] == "needs_review" for x in self.state["jobs"].values()):
            return "needs_review"
        try:
            self.check_time()
            self.youtube.verify_channel()
        except RunDeadline:
            return "run_time_limit"
        except APIError as error:
            reason = "quota_or_upload_limit" if error.reason in QUOTA_REASONS else "api_error"
            self.commit(stop_reason=reason, last_error=error.reason)
            return reason
        except UnknownResult:
            self.commit(stop_reason="unknown_result", last_error="channel_read_network_unknown")
            return "unknown_result"
        # The first actual API upload/publication is the project-eligibility gate.
        has_pilot = any(x["phase"] == "public_api_confirmed" for x in self.state["jobs"].values())
        limit = self.max_jobs if has_pilot else 1
        processed = 0
        for job in self.queue["jobs"]:
            if self.state["jobs"][job["id"]]["phase"] == "public_api_confirmed":
                continue
            if processed >= limit:
                break
            try:
                self.check_time()
                with tempfile.TemporaryDirectory(prefix="course-upload-") as directory:
                    outcome = self.process(job, directory)
                processed += 1
                if outcome == "needs_review":
                    self.commit(stop_reason="needs_review")
                    return "needs_review"
            except StateWriteError:
                raise
            except RunDeadline:
                # SIGALRM/deadline may also prevent a last write. Every mutation
                # already has its durable intent; leave that as recovery truth.
                return "run_time_limit"
            except APIError as error:
                updates = {"last_error": error.reason, "last_http_status": error.status}
                entry = self.state["jobs"][job["id"]]
                # Only an endpoint-specific, explicit quota rejection permits a
                # retry. A GET/refresh failure must never erase a mutation intent.
                if error.reason in QUOTA_REASONS and 400 <= error.status < 500:
                    if error.rejected_operation == "upload" and entry["phase"] == "upload_initiated" and not entry.get("video_id"):
                        updates["phase"] = "pending"
                    elif error.rejected_operation == "caption" and entry["phase"] == "caption_initiated" and not entry.get("caption_id"):
                        updates["phase"] = "uploaded"
                    elif error.rejected_operation == "public" and entry["phase"] == "public_update_initiated":
                        updates.update(phase="processing", public_update_attempted=False)
                self.commit(job, **updates)
                reason = "quota_or_upload_limit" if error.reason in QUOTA_REASONS else "api_error"
                self.commit(stop_reason=reason)
                return reason
            except UnknownResult:
                entry = self.state["jobs"][job["id"]]
                if entry["phase"] == "upload_initiated":
                    self.commit(job, phase="upload_unknown", last_error="unknown_network_result")
                else:
                    self.commit(job, last_error="unknown_network_result")
                self.commit(stop_reason="unknown_result")
                return "unknown_result"
            except ValidationError as error:
                self.needs_review(job, str(error))
                self.commit(stop_reason="needs_review")
                return "needs_review"
        if all(x["phase"] == "public_api_confirmed" for x in self.state["jobs"].values()):
            self.commit(all_done=True, stop_reason="all_done", all_done_at=timestamp())
            return "all_done"
        self.commit(stop_reason="bounded_run_complete")
        return "bounded_run_complete"


def required_environment(name):
    value = os.environ.get(name)
    if not value:
        raise ValidationError("missing_environment_" + name)
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="offline validation (default)")
    mode.add_argument("--preflight", action="store_true", help="read state and confirm OAuth channel only")
    mode.add_argument("--execute", action="store_true", help="run enabled, reviewed queue")
    parser.add_argument("--queue", default="cloud-queue.json")
    parser.add_argument("--state", help="optional local bootstrap state for offline validation")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--max-jobs", type=int, default=MAX_JOBS)
    parser.add_argument("--max-minutes", type=int, default=90)
    args = parser.parse_args(argv)
    try:
        queue = validate_queue(json.loads(Path(args.queue).read_text(encoding="utf-8")))
        if not 1 <= args.max_jobs <= MAX_JOBS or not 1 <= args.max_minutes <= 90:
            raise ValidationError("run_bounds_invalid")
        if not args.execute and not args.preflight:
            if args.state:
                validate_state(json.loads(Path(args.state).read_text(encoding="utf-8")), queue)
            print(json.dumps({"mode": "dry_run", "network_calls": 0, "jobs": len(queue["jobs"]),
                              "enabled": queue["enabled"], "remote_hashes_verified": False,
                              "result": "structure_valid"}))
            return 0
        if args.execute and not queue["enabled"]:
            raise ValidationError("queue_disabled")
        if required_environment("GITHUB_REPOSITORY") != REPOSITORY:
            raise ValidationError("environment_repository_mismatch")
        deadline = time.monotonic() + args.max_minutes * 60
        http = HTTP(deadline)
        store = GitHubStore(http, required_environment("GITHUB_TOKEN"), args.branch)
        state = validate_state(store.load(), queue)
        if args.execute and state["all_done"]:
            print(json.dumps({"mode": "execute", "result": "all_done_noop"}))
            return 0
        youtube = YouTube(http, required_environment("YOUTUBE_CLIENT_ID"), required_environment("YOUTUBE_CLIENT_SECRET"),
                          required_environment("YOUTUBE_REFRESH_TOKEN"))
        if args.preflight:
            youtube.verify_channel()
            print(json.dumps({"mode": "preflight", "result": "target_channel_confirmed",
                              "channel_id": CHANNEL_ID, "state_written": False, "uploaded": False}))
            return 0
        # Unix runner hard wall-clock cap, including streamed socket writes.
        previous_handler = None
        if hasattr(signal, "SIGALRM"):
            previous_handler = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(RunDeadline("run_time_limit")))
            signal.alarm(max(1, int(deadline - time.monotonic())))
        try:
            result = Runner(queue, state, store, youtube, ReleaseAssets(http),
                            args.max_jobs, max(1, deadline - time.monotonic())).run()
        finally:
            if previous_handler is not None:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, previous_handler)
        print(json.dumps({"mode": "execute", "result": result}))
        return 0 if result in {"all_done", "all_done_noop", "bounded_run_complete"} else 2
    except Stop as error:
        print(json.dumps({"result": "stopped", "reason": str(error)}))
        return 2
    except (OSError, ValueError, TypeError, KeyError, HTTPException):
        print(json.dumps({"result": "stopped", "reason": "invalid_local_input_or_unclassified_io_error"}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
