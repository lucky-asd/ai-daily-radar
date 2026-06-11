import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MOBILE_HTML = ROOT / "web" / "mobile.html"
MOBILE_JS = ROOT / "web" / "mobile.js"
MOBILE_CSS = ROOT / "web" / "mobile.css"
WORKFLOW = ROOT / ".github" / "workflows" / "deploy-pages.yml"


class MobileDailyPageTests(unittest.TestCase):
    def test_mobile_page_is_part_of_static_web_artifact(self):
        self.assertTrue(MOBILE_HTML.exists())
        html = MOBILE_HTML.read_text(encoding="utf-8")
        self.assertIn("mobile.css", html)
        self.assertIn("mobile.js", html)
        self.assertIn("viewport-fit=cover", html)
        self.assertIn("date-rail", html)
        self.assertIn("story-title", html)

        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("path: web", workflow)

    def test_mobile_page_reads_existing_digest_and_day_json(self):
        text = MOBILE_JS.read_text(encoding="utf-8")
        self.assertIn("data/digest/index.json", text)
        self.assertIn("data/index.json", text)
        self.assertIn("data/digest/${encodeURIComponent(date)}.json", text)
        self.assertIn("data/day/${encodeURIComponent(date)}.json", text)
        self.assertIn("loadDateIndex", text)
        self.assertIn("publicStories", text)
        self.assertIn("shouldTryDigestData", text)
        self.assertIn('get("date")', text)
        self.assertIn("syncUrlDate", text)
        self.assertIn("firstStoryUrl", text)
        self.assertIn('item.pm_label === "必读"', text)
        self.assertIn('item.pm_label === "值得读"', text)

    def test_mobile_styles_keep_single_column_phone_reader_shape(self):
        css = MOBILE_CSS.read_text(encoding="utf-8")
        self.assertIn("width: min(100%, 520px)", css)
        self.assertIn("grid-template-columns: repeat(3, 1fr)", css)
        self.assertIn("position: fixed", css)
        self.assertIn("env(safe-area-inset-bottom)", css)
        self.assertIn(".date-rail", css)
        self.assertNotIn("min-height: 78svh", css)


if __name__ == "__main__":
    unittest.main()
