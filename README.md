# AI Daily Radar

一个不需要服务器的 AI 日报静态站：GitHub Actions 定时抓公开源，生成 `web/data`，GitHub Pages 直接展示。

## 数据边界

这个公开仓库只放公开展示版：

- 公开 RSS：OpenAI、Hugging Face、Google DeepMind、GitHub AI / Changelog
- 公开线上源：卡兹克精选、卡兹克日报
- 不包含飞书、私有 OPML、API Key、模型配置、私有日志或本地历史数据

本地完整版里的评分、打标签、私有日报和私有源不在这个公开仓库里运行。

## 本地预览

```bash
python3 scripts/rss_sync.py --max-age-days 45
python3 scripts/aihot_sync.py --source-ids aihot-selected,aihot-daily --page-delay-ms 0
python3 scripts/build-web-data.py
cd web
python3 -m http.server 48917
```

打开：

```text
http://localhost:48917/
```

## GitHub Pages

1. 新建仓库，例如 `ai-daily-radar`
2. 推送本目录内容
3. 到仓库设置：`Settings → Pages → Build and deployment → Source: GitHub Actions`
4. 到 `Actions` 手动运行 `Deploy AI Daily Site`

默认每小时更新一次。

## 私有增强版

私有增强版建议另走加密包方案：本地处理飞书/私有源/评分/标签/Digest，然后生成加密的 `private.enc`。公开站输入密码后在浏览器本地解密展示。当前公开站不包含这部分。
