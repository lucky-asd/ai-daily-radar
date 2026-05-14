# AI Daily Radar

一个不需要服务器的 AI 日报静态站：GitHub Actions 定时抓公开源，生成 `web/data`，GitHub Pages 直接展示。

## 数据边界

这个公开仓库只放公开展示版：

- 公开 RSS：OpenAI、Hugging Face、Google DeepMind、GitHub AI / Changelog
- 公开线上源：卡兹克精选、卡兹克日报
- 不包含飞书、私有 OPML、API Key、模型配置、私有日志或本地历史数据

本地完整版里的评分、打标签、私有日报和私有源不会在 GitHub Actions 里运行。

## 本地预览公开版

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

仓库已经内置“私有加密包”入口：页面右上角锁形按钮。它不会把密码发到服务器；密码只在浏览器里用来解密 `web/private/private.enc`。

推荐流程是：

1. 在你的本地完整版里继续抓私有源、评分、打标签、生成日报。
2. 用完整版生成好的 `web/data` 打包加密，并只发布加密后的 `private.enc`：

   ```bash
   PRIVATE_BUNDLE_PASSWORD='换成你自己的强密码' \
     node scripts/publish-private-bundle.mjs \
     --input web/data \
     --output web/private/private.enc \
     --commit \
     --push
   ```

   如果你只是想先在本地生成文件、不提交不推送，就去掉 `--commit` 和 `--push`。

3. 推送后，GitHub Pages 会把 `web/private/private.enc` 作为静态文件一起发布；未加密的 `web/data`、API Key、私有配置不会被上传。
4. 线上打开网页，点右上角锁形按钮，输入密码后显示私有评分、标签和日报。

你也可以先本地验一下密码和加密包是否正确：

```bash
PRIVATE_BUNDLE_PASSWORD='换成你自己的强密码' \
  node scripts/decrypt-private-web-bundle.mjs \
  --input web/private/private.enc \
  --output /tmp/private-bundle-check.json
```

注意：不要把未加密的 `web/data`、API Key、`config.local.json` 推到 GitHub。
