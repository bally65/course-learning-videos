"""No test reads credentials or makes a network request."""
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_upload as worker


VIDEO = b"test video bytes"
SRT = b"1\n00:00:00,000 --> 00:00:01,000\nSample\n"
VIDEO_ID = "testvideo01"


def queue(count=1):
    jobs = []
    for number in range(count):
        jobs.append({"id": "lesson-" + str(number), "title": "Reviewed lesson", "description": "Model limits retained.",
                     "language": "en", "caption_language": "en", "category_id": "27",
                     "sha256": hashlib.sha256(VIDEO).hexdigest(), "srt_sha256": hashlib.sha256(SRT).hexdigest(),
                     "video_url": "https://github.com/" + worker.REPOSITORY + "/releases/download/reviewed/lesson.mp4",
                     "srt_url": "https://github.com/" + worker.REPOSITORY + "/releases/download/reviewed/lesson.srt",
                     "bytes": len(VIDEO), "srt_bytes": len(SRT), "uncertainties": ["Model limits retained."],
                     "content_reviewed": True, "uncertainties_preserved": True,
                     "made_for_kids": False, "contains_synthetic_media": True, "license": "youtube"})
    return {"schema_version": 1, "channel_id": worker.CHANNEL_ID, "repository": worker.REPOSITORY,
            "enabled": True, "initial_published_job_ids": ["previously-published"], "jobs": jobs}


class Store:
    def __init__(self, state, events, fail_on=None):
        self.state, self.events = copy.deepcopy(state), events
        self.writes, self.fail_on = 0, fail_on

    def commit(self, state):
        self.writes += 1
        self.events.append(("commit", copy.deepcopy(state)))
        if self.writes == self.fail_on:
            raise worker.StateWriteError("injected_cas_failure")
        self.state = copy.deepcopy(state)


class Assets:
    def __init__(self, events, bad=False):
        self.events, self.bad = events, bad

    def fetch(self, job, directory, caption_only=False):
        self.events.append(("fetch", caption_only))
        if self.bad:
            raise worker.ValidationError("asset_hash_or_size_mismatch")
        video, srt = Path(directory) / "video.mp4", Path(directory) / "source.srt"
        video.write_bytes(VIDEO)
        srt.write_bytes(SRT)
        return (srt,) if caption_only else (video, srt)


class FakeYouTube:
    def __init__(self, jobs, events):
        self.jobs, self.events = jobs, events
        self.wrong_channel = False
        self.upload_error = None
        self.caption_error = None
        self.public_error = None
        self.reconciled = []
        self.video_id, self.tag = VIDEO_ID, worker.stable_tag(jobs[0])
        self.public, self.accept_public = False, True
        self.processing = "succeeded"
        self.captions = []

    def verify_channel(self):
        self.events.append(("verify_channel",))
        if self.wrong_channel:
            raise worker.ValidationError("wrong_channel")

    def reconcile_upload(self, tag):
        self.events.append(("reconcile", tag))
        return self.reconciled

    def upload_private(self, job, path, tag):
        self.events.append(("upload", job["id"]))
        self.tag = tag
        if self.upload_error:
            raise self.upload_error
        return self.video_id

    def get_video(self, video_id):
        self.events.append(("get_video", video_id))
        return {"id": video_id, "snippet": {"channelId": worker.CHANNEL_ID, "tags": [self.tag]},
                "status": {"privacyStatus": "public" if self.public else "private", "uploadStatus": "processed"},
                "processingDetails": {"processingStatus": self.processing}}

    def list_captions(self, video_id):
        self.events.append(("list_captions", video_id))
        return copy.deepcopy(self.captions)

    def add_caption(self, job, status="serving"):
        self.captions = [{"id": "caption_id", "snippet": {"name": worker.caption_name(job),
                          "language": job["caption_language"], "status": status, "isDraft": False}}]

    def insert_caption(self, job, video_id, srt_path):
        self.events.append(("insert_caption", video_id))
        self.add_caption(job)
        if self.caption_error:
            raise self.caption_error
        return "caption_id"

    def set_public(self, video):
        self.events.append(("set_public", video["id"]))
        if self.accept_public:
            self.public = True
        if self.public_error:
            raise self.public_error


class Response(io.BytesIO):
    def __init__(self, data, status=200, headers=None):
        super().__init__(data)
        self.status, self.headers = status, headers or {}


class AssetHTTP:
    def __init__(self, bad=False):
        self.bad, self.calls = bad, []

    def check_time(self):
        pass

    def request(self, method, url, **kwargs):
        self.calls.append((method, url))
        data = SRT if url.endswith(".srt") else VIDEO
        return Response(b"bad" if self.bad else data)


class WorkerTests(unittest.TestCase):
    def setup_worker(self, count=1, state=None, fail_on=None):
        q, events = queue(count), []
        state = worker.empty_state(q) if state is None else state
        store = Store(state, events, fail_on)
        api, assets = FakeYouTube(q["jobs"], events), Assets(events)
        runner = worker.Runner(q, state, store, api, assets)
        return q, events, store, api, assets, runner

    def names(self, events):
        return [event[0] for event in events]

    def test_wrong_channel_prevents_download_and_upload(self):
        _, events, store, api, _, runner = self.setup_worker()
        api.wrong_channel = True
        with self.assertRaises(worker.ValidationError):
            runner.run()
        self.assertEqual(self.names(events), ["verify_channel"])
        self.assertEqual(store.writes, 0)

    def test_actual_hash_mismatch_prevents_upload(self):
        _, events, store, _, _, runner = self.setup_worker()
        runner.assets = worker.ReleaseAssets(AssetHTTP(bad=True))
        self.assertEqual(runner.run(), "needs_review")
        self.assertNotIn("upload", self.names(events))
        self.assertEqual(store.state["jobs"]["lesson-0"]["review_reason"], "asset_hash_or_size_mismatch")

    def test_actual_hash_and_size_verification(self):
        q = queue()
        with tempfile.TemporaryDirectory() as directory:
            paths = worker.ReleaseAssets(AssetHTTP()).fetch(q["jobs"][0], directory)
            self.assertEqual(paths[0].read_bytes(), VIDEO)
            self.assertEqual(paths[1].read_bytes(), SRT)
        q["jobs"][0]["bytes"] += 1
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(worker.ValidationError):
            worker.ReleaseAssets(AssetHTTP()).fetch(q["jobs"][0], directory)

    def test_unknown_upload_never_blindly_repeats(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.upload_error = worker.UnknownResult("unknown")
        self.assertEqual(runner.run(), "unknown_result")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "upload_unknown")
        api.upload_error = None
        resumed = worker.Runner(q, store.state, store, api, assets)
        self.assertEqual(resumed.run(), "needs_review")
        self.assertEqual(self.names(events).count("upload"), 1)
        self.assertIn("reconcile", self.names(events))

    def test_crash_after_intent_is_reconciled_without_upload(self):
        q, events, store, api, assets, _ = self.setup_worker()
        store.state["jobs"]["lesson-0"]["phase"] = "upload_initiated"
        api.reconciled = [VIDEO_ID]
        runner = worker.Runner(q, store.state, store, api, assets)
        self.assertEqual(runner.run(), "all_done")
        self.assertNotIn("upload", self.names(events))
        self.assertEqual([x[1] for x in events if x[0] == "fetch"], [True])

    def test_ambiguous_or_truncated_scan_stops(self):
        for matches in ([], [VIDEO_ID, "othervideo1"], None):
            q, events, store, api, assets, _ = self.setup_worker()
            store.state["jobs"]["lesson-0"]["phase"] = "upload_initiated"
            api.reconciled = matches
            result = worker.Runner(q, store.state, store, api, assets).run()
            self.assertEqual(result, "needs_review")
            self.assertNotIn("upload", self.names(events))

    def test_caption_dedup_with_existing_source_name(self):
        q, events, store, api, _, runner = self.setup_worker()
        api.add_caption(q["jobs"][0])
        self.assertEqual(runner.run(), "all_done")
        self.assertNotIn("insert_caption", self.names(events))
        self.assertEqual(store.state["jobs"]["lesson-0"]["caption_id"], "caption_id")

    def test_unknown_caption_response_lists_before_retry(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.caption_error = worker.UnknownResult("unknown")
        self.assertEqual(runner.run(), "unknown_result")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "caption_initiated")
        api.caption_error = None
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("insert_caption"), 1)

    def test_unknown_caption_absent_stops_without_duplicate(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.caption_error = worker.UnknownResult("unknown")
        runner.run()
        api.caption_error, api.captions = None, []
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "needs_review")
        self.assertEqual(self.names(events).count("insert_caption"), 1)

    def test_quota_and_upload_limit_stop_entire_run(self):
        for reason in worker.QUOTA_REASONS:
            _, events, store, api, _, runner = self.setup_worker(3)
            api.upload_error = worker.APIError(reason, 403)
            self.assertEqual(runner.run(), "quota_or_upload_limit")
            self.assertEqual(self.names(events).count("upload"), 1)
            self.assertEqual(store.state["stop_reason"], "quota_or_upload_limit")

    def test_explicit_rejected_upload_quota_can_resume_next_run(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.upload_error = worker.APIError("uploadLimitExceeded", 403, rejected_operation="upload")
        self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "pending")
        api.upload_error = None
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("upload"), 2)
        self.assertNotIn("reconcile", self.names(events))

    def test_explicit_rejected_caption_quota_can_resume_next_run(self):
        q, events, store, api, assets, runner = self.setup_worker()
        with patch.object(api, "insert_caption", side_effect=worker.APIError("quotaExceeded", 403, rejected_operation="caption")):
            self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "uploaded")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("upload"), 1)
        self.assertEqual(self.names(events).count("insert_caption"), 1)

    def test_explicit_rejected_public_quota_can_resume_next_run(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.accept_public = False
        api.public_error = worker.APIError("quotaExceeded", 403, rejected_operation="public")
        self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertFalse(store.state["jobs"]["lesson-0"]["public_update_attempted"])
        api.accept_public, api.public_error = True, None
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("set_public"), 2)

    def test_get_quota_after_public_put_keeps_attempt_protection(self):
        q, events, store, api, assets, runner = self.setup_worker()
        original_get = api.get_video
        calls = 0
        def read_then_quota(video_id):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise worker.APIError("quotaExceeded", 403)
            return original_get(video_id)
        with patch.object(api, "get_video", side_effect=read_then_quota):
            self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertTrue(store.state["jobs"]["lesson-0"]["public_update_attempted"])
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "public_update_initiated")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("set_public"), 1)

    def test_get_quota_after_caption_put_keeps_existing_caption(self):
        q, events, store, api, assets, runner = self.setup_worker()
        original_list = api.list_captions
        calls = 0
        def list_then_quota(video_id):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise worker.APIError("quotaExceeded", 403)
            return original_list(video_id)
        with patch.object(api, "list_captions", side_effect=list_then_quota):
            self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "caption_attached")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("insert_caption"), 1)

    def test_public_api_confirmation_never_claims_playback(self):
        q, events, store, api, assets, runner = self.setup_worker()
        self.assertEqual(runner.run(), "all_done")
        entry = store.state["jobs"]["lesson-0"]
        self.assertEqual(entry["phase"], "public_api_confirmed")
        self.assertEqual(entry["playback_verification"], "not_performed")
        self.assertNotIn("public_verified", json.dumps(store.state))
        events.clear()
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done_noop")
        self.assertEqual(events, [])

    def test_public_update_unknown_reconciles_before_mutation(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.public_error = worker.UnknownResult("unknown")
        self.assertEqual(runner.run(), "unknown_result")
        api.public_error = None
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "all_done")
        self.assertEqual(self.names(events).count("set_public"), 1)

    def test_public_update_readback_private_is_not_completion(self):
        _, events, store, api, _, runner = self.setup_worker()
        api.accept_public = False
        self.assertEqual(runner.run(), "needs_review")
        self.assertFalse(store.state["all_done"])
        self.assertNotEqual(store.state["jobs"]["lesson-0"]["phase"], "public_api_confirmed")

    def test_public_unknown_then_caption_syncing_never_replays_public_put(self):
        q, events, store, api, assets, runner = self.setup_worker()
        api.accept_public = False
        api.public_error = worker.UnknownResult("unknown")
        self.assertEqual(runner.run(), "unknown_result")
        self.assertTrue(store.state["jobs"]["lesson-0"]["public_update_attempted"])
        api.public_error = None
        api.add_caption(q["jobs"][0], status="syncing")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "bounded_run_complete")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "processing")
        api.add_caption(q["jobs"][0], status="serving")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "needs_review")
        self.assertEqual(self.names(events).count("set_public"), 1)

    def test_legacy_public_intent_survives_processing_phase_change(self):
        q, events, store, api, assets, _ = self.setup_worker()
        store.state["jobs"]["lesson-0"].update(phase="public_update_initiated", video_id=VIDEO_ID)
        api.add_caption(q["jobs"][0], status="syncing")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "bounded_run_complete")
        self.assertTrue(store.state["jobs"]["lesson-0"]["public_update_attempted"])
        api.add_caption(q["jobs"][0], status="serving")
        self.assertEqual(worker.Runner(q, store.state, store, api, assets).run(), "needs_review")
        self.assertNotIn("set_public", self.names(events))

    def test_channel_check_quota_is_persisted(self):
        _, events, store, api, _, runner = self.setup_worker()
        with patch.object(api, "verify_channel", side_effect=worker.APIError("quotaExceeded", 403)):
            self.assertEqual(runner.run(), "quota_or_upload_limit")
        self.assertEqual(store.state["stop_reason"], "quota_or_upload_limit")
        self.assertNotIn("upload", self.names(events))

    def test_cas_failure_before_upload_prevents_upload(self):
        _, events, _, _, _, runner = self.setup_worker(fail_on=1)
        with self.assertRaises(worker.StateWriteError):
            runner.run()
        self.assertNotIn("upload", self.names(events))

    def test_cas_failure_after_video_id_prevents_caption_calls(self):
        _, events, store, _, _, runner = self.setup_worker(fail_on=2)
        with self.assertRaises(worker.StateWriteError):
            runner.run()
        self.assertEqual(self.names(events).count("upload"), 1)
        self.assertNotIn("get_video", self.names(events))
        self.assertNotIn("insert_caption", self.names(events))
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "upload_initiated")

    def test_cas_failure_before_caption_prevents_insert(self):
        _, events, _, _, _, runner = self.setup_worker(fail_on=3)
        with self.assertRaises(worker.StateWriteError):
            runner.run()
        self.assertNotIn("insert_caption", self.names(events))

    def test_cas_failure_before_public_prevents_public(self):
        _, events, _, _, _, runner = self.setup_worker(fail_on=5)
        with self.assertRaises(worker.StateWriteError):
            runner.run()
        self.assertNotIn("set_public", self.names(events))

    def test_processing_wait_is_bounded_and_keeps_private(self):
        _, events, store, api, _, runner = self.setup_worker()
        api.processing = "processing"
        self.assertEqual(runner.run(), "bounded_run_complete")
        self.assertEqual(store.state["jobs"]["lesson-0"]["phase"], "processing")
        self.assertEqual(self.names(events).count("get_video"), 1)
        self.assertNotIn("set_public", self.names(events))

    def test_first_run_is_one_job_pilot(self):
        _, events, store, _, _, runner = self.setup_worker(8)
        self.assertEqual(runner.run(), "bounded_run_complete")
        self.assertEqual(self.names(events).count("upload"), 1)
        self.assertEqual(sum(x["phase"] == "public_api_confirmed" for x in store.state["jobs"].values()), 1)

    def test_later_run_is_at_most_five_jobs(self):
        q, events, store, api, assets, _ = self.setup_worker(8)
        store.state["jobs"]["lesson-0"].update(phase="public_api_confirmed", video_id=VIDEO_ID)
        self.assertEqual(worker.Runner(q, store.state, store, api, assets, max_jobs=999).run(), "bounded_run_complete")
        self.assertEqual(self.names(events).count("upload"), 5)

    def test_deadline_prevents_external_mutation(self):
        _, events, _, _, _, runner = self.setup_worker()
        runner.deadline = 0
        self.assertEqual(runner.run(), "run_time_limit")
        self.assertNotIn("upload", self.names(events))

    def test_excluded_job_and_changed_content_are_rejected(self):
        q = queue()
        q["initial_published_job_ids"].append(q["jobs"][0]["id"])
        with self.assertRaises(worker.ValidationError):
            worker.validate_queue(q)
        q = queue()
        state = worker.empty_state(q)
        q["jobs"][0]["description"] += " changed"
        with self.assertRaises(worker.ValidationError):
            worker.validate_state(state, q)

    def test_uncertainties_are_allowed_and_preserved(self):
        q = queue()
        self.assertEqual(worker.validate_queue(q)["jobs"][0]["uncertainties"], ["Model limits retained."])
        q["jobs"][0]["uncertainties_preserved"] = False
        with self.assertRaises(worker.ValidationError):
            worker.validate_queue(q)

    def test_asset_url_allowlist(self):
        urls = ["http://github.com/bally65/course-learning-videos/releases/download/r/a.mp4",
                "https://github.com/elsewhere/repo/releases/download/r/a.mp4",
                "https://github.com/bally65/course-learning-videos/releases/download/r/%2e%2e",
                "https://github.com/bally65/course-learning-videos/releases/download/r/a.mp4?token=x",
                "https://github.com.evil.test/bally65/course-learning-videos/releases/download/r/a.mp4"]
        for url in urls:
            with self.assertRaises(worker.ValidationError):
                worker.validate_asset_url(url)

    def test_default_dry_run_has_no_network_or_environment_access(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            path.write_text(json.dumps(queue()))
            with patch.object(worker, "required_environment", side_effect=AssertionError("must not read env")), \
                 patch.object(worker.HTTP, "request", side_effect=AssertionError("must not call network")), \
                 patch("sys.stdout", new_callable=io.StringIO) as output:
                self.assertEqual(worker.main(["--queue", str(path)]), 0)
                self.assertEqual(json.loads(output.getvalue())["network_calls"], 0)

    def test_disabled_execute_does_not_read_credentials(self):
        q = queue()
        q["enabled"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            path.write_text(json.dumps(q))
            with patch.object(worker, "required_environment", side_effect=AssertionError("must not read env")), \
                 patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(worker.main(["--queue", str(path), "--execute"]), 2)

    def test_missing_local_input_is_sanitized(self):
        with tempfile.TemporaryDirectory() as directory, patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(worker.main(["--queue", str(Path(directory) / "missing.json")]), 2)
            self.assertEqual(json.loads(output.getvalue())["reason"], "invalid_local_input_or_unclassified_io_error")

    def test_preflight_only_reads_state_and_verifies_channel(self):
        q = queue()
        q["enabled"] = False
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.json"
            path.write_text(json.dumps(q))
            state = worker.empty_state(q)
            with patch.object(worker, "required_environment", side_effect=lambda name: worker.REPOSITORY if name == "GITHUB_REPOSITORY" else "FAKE"), \
                 patch.object(worker, "GitHubStore") as store, patch.object(worker, "YouTube") as youtube, \
                 patch.object(worker, "ReleaseAssets", side_effect=AssertionError("no assets")), \
                 patch("sys.stdout", new_callable=io.StringIO):
                store.return_value.load.return_value = state
                self.assertEqual(worker.main(["--queue", str(path), "--preflight"]), 0)
                store.return_value.commit.assert_not_called()
                youtube.return_value.verify_channel.assert_called_once_with()
                self.assertEqual(len(youtube.return_value.mock_calls), 1)

    def test_github_commit_uses_loaded_sha_without_retry(self):
        from unittest.mock import Mock
        http = Mock()
        http.json.return_value = {"encoding": "base64", "sha": "a" * 40,
                                  "content": worker.base64.b64encode(b"{}").decode()}
        store = worker.GitHubStore(http, "FAKE")
        self.assertEqual(store.load(), {})
        http.json.side_effect = worker.APIError("conflict", 409)
        with self.assertRaises(worker.StateWriteError):
            store.commit({"safe": True})
        self.assertEqual(http.json.call_count, 2)
        self.assertEqual(http.json.call_args.kwargs["value"]["sha"], "a" * 40)

    def test_youtube_channel_match_requires_exactly_one(self):
        from unittest.mock import Mock
        api = worker.YouTube(Mock(), "FAKE", "FAKE", "FAKE")
        for items in ([], [{"id": "wrong"}], [{"id": worker.CHANNEL_ID}, {"id": "other"}]):
            with patch.object(api, "call", return_value={"items": items}), self.assertRaises(worker.ValidationError):
                api.verify_channel()

    def test_http_marks_only_explicit_rejected_quota_mutation(self):
        from unittest.mock import Mock
        cases = [("POST", worker.UPLOAD_BASE + "videos", "upload"),
                 ("PUT", worker.UPLOAD_BASE + "videos?upload_id=FAKE", "upload"),
                 ("POST", worker.UPLOAD_BASE + "captions", "caption"),
                 ("PUT", worker.API_BASE + "videos", "public"),
                 ("GET", worker.API_BASE + "videos", None),
                 ("GET", worker.API_BASE + "captions", None),
                 ("POST", "https://oauth2.googleapis.com/token", None)]
        for method, url, operation in cases:
            with self.subTest(method=method, endpoint=url):
                http = worker.HTTP()
                body = io.BytesIO(worker.canonical({"error": {"errors": [{"reason": "quotaExceeded"}]}}))
                http.opener = Mock()
                http.opener.open.side_effect = worker.HTTPError(url, 403, "forbidden", {}, body)
                with self.assertRaises(worker.APIError) as raised:
                    http.request(method, url)
                self.assertEqual(raised.exception.rejected_operation, operation)
                self.assertEqual(http.opener.open.call_count, 1)

    def test_http_5xx_mutation_is_unknown_and_delete_is_forbidden(self):
        from unittest.mock import Mock
        http = worker.HTTP()
        http.opener = Mock()
        http.opener.open.side_effect = worker.HTTPError(worker.API_BASE + "videos", 503, "unavailable", {}, io.BytesIO(b"{}"))
        with self.assertRaises(worker.UnknownResult):
            http.request("PUT", worker.API_BASE + "videos")
        http.opener.reset_mock()
        with self.assertRaises(worker.ValidationError):
            http.request("DELETE", worker.API_BASE + "videos")
        http.opener.open.assert_not_called()


if __name__ == "__main__":
    unittest.main()
