import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RSS_SYNC = ROOT / "scripts" / "rss_sync.py"


def load_rss_sync():
    spec = importlib.util.spec_from_file_location("rss_sync", RSS_SYNC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class RssTimezoneTests(unittest.TestCase):
    def test_rss_day_buckets_use_china_local_date_for_static_daily(self):
        mod = load_rss_sync()
        dt = datetime(2026, 5, 13, 19, 27, tzinfo=timezone.utc)
        self.assertEqual("2026-05-14", mod._rss_date_str(dt))

    def test_rss_day_buckets_keep_early_utc_same_local_date(self):
        mod = load_rss_sync()
        dt = datetime(2026, 5, 13, 11, 0, tzinfo=timezone.utc)
        self.assertEqual("2026-05-13", mod._rss_date_str(dt))


if __name__ == "__main__":
    unittest.main()
