# 關機後接續上傳課程影片

這個倉庫保存雲端上傳流程。實際開關見 cloud-queue.json，逐支進度見 cloud-state.json；API 公開回讀與實際播放驗收分開記錄。

## 已凍結範圍

目標是 bally，頻道 UCqHmPyl4dRExH1pTBeMuqBA。cloud-queue.json 只含剩下 28 支已核對成品與各一軌字幕，影片約 3.85 GB。原錄音、原試卷掃描、帳號資料與本機完整離線包不在上傳清單。先前已發布的 16 支列為排除項。旁白、字幕、來源與待核對標示沿用已完成版本。

## 電腦可以何時關機

完整成品傳到 GitHub Release、Google 頻道完成授權並通過雲端連線驗證、首輪雲端執行留下可核對紀錄，而且本機舊排程停用後，剩下工作就不再依賴這台電腦。首輪若遇到明確的 YouTube 每日上限，保留待重試狀態；這不代表第一支影片已成功公開。

## 雲端流程

GitHub 標準 Ubuntu hosted runner → 讀取固定工作清單及進度 → 驗證成品 SHA256 → 核對 Google 回傳的本人頻道 → 上傳私人影片 → 等待轉碼及字幕 → 設為公開 → 讀回公開狀態 → 保存進度。

首次最多一支；實際 API 公開回讀完成後才逐批接續，每次最多五支。預計每天台北時間 23:17，GitHub 繁忙時可能延後。遇 YouTube 頻道上限或 API 配額就停止當次；不換帳號或專案繞過。不確定上次是否上傳成功時，先查本人上傳清單，不能猜測重傳。

`public_api_confirmed` 只表示 YouTube API 的公開狀態、轉碼及字幕查驗，不冒充逐支實際播放或真人試聽。原成片的既有音質驗收仍保留。

## 一次性授權與啟用

1. 在 Google Cloud 為專用 OAuth 用戶端啟用 YouTube Data API。採適合部署平台的 OAuth client，使用者透過 Google 官方授權畫面授權本人頻道。External Testing 的 YouTube refresh token 會在七天後失效；要長期執行需確認適當的發布狀態。
2. 完整流程所需的單一 scope 是 `youtube.force-ssl`。Google 對此權限的文字包含查看、編輯與刪除 YouTube 內容；本程式只實作列舉、上傳、字幕和本清單影片的公開設定，沒有刪除端點。授權會新增雲端持續存取頻道的能力，需要本人確認一次。
3. 僅將 OAuth client ID、client secret、refresh token 存入本倉庫的 GitHub Actions Secrets，名稱是 YOUTUBE_CLIENT_ID、YOUTUBE_CLIENT_SECRET、YOUTUBE_REFRESH_TOKEN。不得放入 Git、雲端公開進度、日誌或對話；不使用瀏覽器 Cookie。
4. 上傳 SHA 相符的成品到同倉庫 Release；先跑 dry-run 和唯讀 preflight，確認頻道 ID。接著啟用 queue.enabled 與 YOUTUBE_CLOUD_ENABLED，首次最多一支。沒有實際通過這步就不宣布雲端可用。
5. 切換執行前停用本機 automation-2，避免兩個系統同時上傳。將雲端進度合併回本機時，public_api_confirmed 與播放已核對保持不同狀態。
6. 全部 API 公開確認後寫 all_done；後續不再上傳。可在 GitHub Actions 停用工作流程；撤銷 Google 授權會停止新的 API 存取。

## 費用與限制

此方案選現有公開倉庫的標準 GitHub-hosted runner，不建立付費 VM、Cloud Run 或 Cloud Storage。官方說明標準 public runner 免費；不代表所有外部服務永久免費。YouTube 的頻道每日上限仍適用，與 API 配額是不同限制。

## 官方資料

- https://developers.openai.com/training/walkthroughs/scheduled-tasks
- https://docs.github.com/en/actions/concepts/billing-and-usage
- https://docs.github.com/en/actions/concepts/security/secrets
- https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule
- https://developers.google.com/youtube/data/reference/rest/v3/videos/insert
- https://developers.google.com/youtube/data/reference/rest/v3/captions/insert
- https://developers.google.com/identity/protocols/oauth2#expiration
- https://support.google.com/youtube/answer/10383400?hl=en

2026-10-19 查詢時，現行 API reference 與舊版 YouTube Help 對未審核 project 的公開限制有衝突，因此以實際一支上傳／公開驗證為準，不以舊規則猜測成功或失敗。
