import json
import os
import unittest


REQUIRED_COUNT_KEYS = {
    "lightning_mentions",
    "bolt11_mentions",
    "lnurl_mentions",
    "phoenixd_mentions",
    "tipjar_wellknown_mentions",
}


class TestAdoptionJson(unittest.TestCase):
    def setUp(self):
        path = os.path.join("lightning", "data", "adoption.json")
        with open(path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    def test_top_level(self):
        self.assertIn("updated_at", self.data)
        self.assertIn("sources", self.data)
        self.assertIsInstance(self.data["sources"], dict)

    def _assert_source_shape(self, key: str):
        src = self.data["sources"].get(key)
        self.assertIsNotNone(src, f"missing sources.{key}")

        self.assertIn("mode", src)
        self.assertIn("counts", src)
        self.assertIn("meta", src)
        self.assertIn("highlights", src)

        counts = src["counts"]
        self.assertTrue(REQUIRED_COUNT_KEYS.issubset(set(counts.keys())))

        # At least one scanned counter present.
        self.assertTrue(
            ("posts_scanned" in counts) or ("pages_scanned" in counts),
            f"sources.{key}.counts missing posts_scanned/pages_scanned",
        )

        # JSON-serializable highlights.
        self.assertIsInstance(src["highlights"], list)
        for h in src["highlights"]:
            self.assertIn("url", h)

    def test_sources_present(self):
        self._assert_source_shape("moltx")
        self._assert_source_shape("hotmolts")

    def test_hotmolts_graceful_failure_is_ok(self):
        src = self.data["sources"].get("hotmolts")
        self.assertIsNotNone(src)
        # Either it worked or it failed gracefully, but it should not crash the pipeline.
        self.assertIn(src.get("mode"), {"cached_html_list", "failed_gracefully"})


if __name__ == "__main__":
    unittest.main()
