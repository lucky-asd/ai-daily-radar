import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "web" / "app.js"
WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pages.yml"
CONFIG = ROOT / "config.yaml"


class GitHubPagesStaticModeTests(unittest.TestCase):
    def test_deploy_workflow_builds_web_and_deploys_pages_artifact(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("actions/configure-pages", text)
        self.assertIn("actions/upload-pages-artifact", text)
        self.assertIn("actions/deploy-pages", text)
        self.assertIn("python3 scripts/rss_sync.py", text)
        self.assertIn("python3 scripts/build-web-data.py", text)
        self.assertIn("path: web", text)

    def test_local_static_preview_port_is_static_mode(self):
        text = APP_JS.read_text(encoding="utf-8")
        self.assertIn('STATIC_PREVIEW_PORTS = new Set(["48917"])', text)
        self.assertIn('STATIC_PREVIEW_PORTS.has(window.location.port || "")', text)

    def test_static_mode_skips_backend_api_boot_requirements(self):
        text = APP_JS.read_text(encoding="utf-8")
        self.assertIn("const IS_STATIC_SITE", text)
        self.assertIn("function applyStaticModeChrome", text)
        self.assertIn("if (IS_STATIC_SITE) return", text)
        self.assertIn("applyStaticModeChrome();", text)
        self.assertIn("STATIC_SITE_SOURCE_STATUS", text)

    def test_static_mode_reads_digest_from_static_json_instead_of_api(self):
        text = APP_JS.read_text(encoding="utf-8")
        self.assertIn("data/digest/${encodeURIComponent(date)}.json", text)
        self.assertIn("if (IS_STATIC_SITE) {", text)

    def test_workflow_limits_rss_history_for_daily_site(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        rss_sync = (ROOT / "scripts" / "rss_sync.py").read_text(encoding="utf-8")
        self.assertIn("--max-age-days 45", workflow)
        self.assertIn("--max-age-days", rss_sync)
        self.assertIn("max_age_days", rss_sync)


    def test_static_mode_hides_local_scoring_tags_and_category_surfaces(self):
        text = APP_JS.read_text(encoding="utf-8")
        css = (ROOT / "web" / "style.css").read_text(encoding="utf-8")
        self.assertIn('document.documentElement.toggleAttribute("data-static-site", IS_STATIC_SITE)', text)
        self.assertIn('[data-static-site] .title-score', css)
        self.assertIn('[data-static-site] .level-row', css)
        self.assertIn('[data-static-site] .tag-row', css)
        self.assertIn('[data-static-site] .side-section[data-static-hide="true"]', css)
        self.assertIn('setStaticHiddenSection("CATEGORIES")', text)
        self.assertIn('setStaticHiddenSection("WORTH READING")', text)
        self.assertIn('setStaticHiddenSection("ENTITY TAGS")', text)
        self.assertIn('setStaticHiddenSection("TOPIC TAGS")', text)

    def test_public_config_includes_aihot_sources(self):
        text = CONFIG.read_text(encoding="utf-8")
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("id: aihot-selected", text)
        self.assertIn("id: aihot-daily", text)
        self.assertIn("type: aihot_api", text)
        self.assertIn("python3 scripts/aihot_sync.py", workflow)

    def test_public_rss_sources_exist_for_action_without_feishu(self):
        text = CONFIG.read_text(encoding="utf-8")
        self.assertIn("id: rss-openai-news", text)
        self.assertIn("type: rss", text)
        self.assertIn("https://openai.com/news/rss.xml", text)
        self.assertIn("https://huggingface.co/blog/feed.xml", text)


if __name__ == "__main__":
    unittest.main()
