import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUNDLE_SCRIPT = ROOT / "scripts" / "build-private-web-bundle.mjs"
APP_JS = ROOT / "web" / "app.js"
INDEX_HTML = ROOT / "web" / "index.html"


class PrivateBundleTests(unittest.TestCase):
    def test_bundle_script_encrypts_static_web_data_without_plaintext(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data = root / "web" / "data"
            (data / "day").mkdir(parents=True)
            (data / "digest").mkdir(parents=True)
            (data / "index.json").write_text(json.dumps({
                "generated_at": "2026-05-13T00:00:00Z",
                "sources": [{"id": "private", "name": "Private"}],
                "categories": [],
                "days": [{"date": "2026-05-13", "items": 1, "cards": 0}],
            }), encoding="utf-8")
            (data / "day" / "2026-05-13.json").write_text(json.dumps({
                "date": "2026-05-13",
                "cards": [],
                "items": [{"item_id": "secret-1", "title": "Very Secret Item", "source": "Private"}],
            }), encoding="utf-8")
            (data / "digest" / "index.json").write_text(json.dumps({"dates": []}), encoding="utf-8")
            out = root / "web" / "private" / "private.enc"
            env = {**os.environ, "PRIVATE_BUNDLE_PASSWORD": "correct horse battery staple"}
            subprocess.run([
                "node", str(BUNDLE_SCRIPT),
                "--input", str(data),
                "--output", str(out),
            ], check=True, env=env, cwd=ROOT)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(1, payload["version"])
            self.assertEqual("AES-GCM", payload["algorithm"])
            self.assertEqual("gzip", payload["compression"])
            self.assertEqual("PBKDF2", payload["kdf"]["name"])
            self.assertIn("ciphertext", payload)
            self.assertNotIn("Very Secret Item", out.read_text(encoding="utf-8"))
            decrypted = root / "decrypted.json"
            subprocess.run([
                "node", str(ROOT / "scripts" / "decrypt-private-web-bundle.mjs"),
                "--input", str(out),
                "--output", str(decrypted),
            ], check=True, env=env, cwd=ROOT)
            self.assertIn("Very Secret Item", decrypted.read_text(encoding="utf-8"))


    def test_bundle_script_can_limit_recent_days(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data = root / "web" / "data"
            (data / "day").mkdir(parents=True)
            (data / "digest").mkdir(parents=True)
            dates = ["2026-05-15", "2026-05-14", "2026-05-13"]
            (data / "index.json").write_text(json.dumps({
                "generated_at": "2026-05-15T00:00:00Z",
                "sources": [],
                "categories": [],
                "days": [{"date": d, "items": 1, "cards": 0} for d in dates],
            }), encoding="utf-8")
            for date in dates:
                (data / "day" / f"{date}.json").write_text(json.dumps({
                    "date": date,
                    "cards": [],
                    "items": [{"item_id": date, "title": date}],
                }), encoding="utf-8")
                (data / "digest" / f"{date}.json").write_text(json.dumps({"date": date, "summary": date}), encoding="utf-8")
            (data / "digest" / "index.json").write_text(json.dumps({"dates": dates}), encoding="utf-8")
            out = root / "web" / "private" / "private.enc"
            decrypted = root / "decrypted.json"
            env = {**os.environ, "PRIVATE_BUNDLE_PASSWORD": "correct horse battery staple"}
            subprocess.run([
                "node", str(BUNDLE_SCRIPT),
                "--input", str(data),
                "--output", str(out),
                "--max-days", "2",
            ], check=True, env=env, cwd=ROOT)
            subprocess.run([
                "node", str(ROOT / "scripts" / "decrypt-private-web-bundle.mjs"),
                "--input", str(out),
                "--output", str(decrypted),
            ], check=True, env=env, cwd=ROOT)
            payload = json.loads(decrypted.read_text(encoding="utf-8"))
            self.assertEqual(["2026-05-15", "2026-05-14"], [d["date"] for d in payload["index"]["days"]])
            self.assertEqual({"2026-05-15", "2026-05-14"}, set(payload["days"].keys()))
            self.assertEqual(["2026-05-15", "2026-05-14"], payload["digest_index"]["dates"])

    def test_frontend_has_private_unlock_flow(self):
        app = APP_JS.read_text(encoding="utf-8")
        html = INDEX_HTML.read_text(encoding="utf-8")
        self.assertIn('id="private-unlock-btn"', html)
        self.assertIn("async function unlockPrivateBundle", app)
        self.assertIn("data/private/private.enc", app)
        self.assertIn("decryptPrivateBundle", app)
        self.assertIn("data-private-unlocked", app)
        self.assertIn("state.privateBundle", app)


if __name__ == "__main__":
    unittest.main()
