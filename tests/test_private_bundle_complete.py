import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP_JS = ROOT / "web" / "app.js"
INDEX_HTML = ROOT / "web" / "index.html"
README = ROOT / "README.md"
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-private-bundle.mjs"
DECRYPT_SCRIPT = ROOT / "scripts" / "decrypt-private-web-bundle.mjs"


class PrivateBundleCompleteTests(unittest.TestCase):
    def test_private_unlock_uses_modal_not_browser_prompt(self):
        html = INDEX_HTML.read_text(encoding="utf-8")
        app = APP_JS.read_text(encoding="utf-8")
        self.assertIn('id="private-unlock-modal"', html)
        self.assertIn('id="private-password"', html)
        self.assertIn('id="private-unlock-submit"', html)
        self.assertIn('id="private-unlock-status"', html)
        self.assertIn("function openPrivateUnlockModal", app)
        self.assertIn("function closePrivateUnlockModal", app)
        self.assertNotIn("window.prompt", app)

    def test_private_bundle_has_local_publish_and_decrypt_tools(self):
        self.assertTrue(PUBLISH_SCRIPT.exists())
        self.assertTrue(DECRYPT_SCRIPT.exists())
        publish = PUBLISH_SCRIPT.read_text(encoding="utf-8")
        decrypt = DECRYPT_SCRIPT.read_text(encoding="utf-8")
        self.assertIn("PRIVATE_BUNDLE_PASSWORD", publish)
        self.assertIn("web/private/private.enc", publish)
        self.assertIn("git", publish)
        self.assertIn("private.enc", decrypt)
        self.assertIn("AES-GCM", decrypt)

    def test_readme_documents_end_to_end_private_publish_flow(self):
        readme = README.read_text(encoding="utf-8")
        self.assertIn("publish-private-bundle.mjs", readme)
        self.assertIn("decrypt-private-web-bundle.mjs", readme)
        self.assertIn("--commit", readme)
        self.assertIn("--push", readme)


if __name__ == "__main__":
    unittest.main()
