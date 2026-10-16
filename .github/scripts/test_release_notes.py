import importlib.util
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location("release_notes", Path(__file__).with_name("release-notes.py"))
NOTES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NOTES)


class ReleaseNotesTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "wiki/changes").mkdir(parents=True)
        (self.root / "wiki/feature.md").write_text("# Feature\n", encoding="utf-8")

    def add_changelog(self, date, body):
        path = self.root / f"wiki/changes/changes-in-alisql-8.0.44.{date}-en.md"
        path.write_text("# Release\n\n[ [Navigation](../feature.md) ]\n\n" + body, encoding="utf-8")
        path.with_name(path.name.replace("-en.md", "-zh.md")).write_text("# Translation\n", encoding="utf-8")
        return path

    def render(self, **kwargs):
        return NOTES.render_notes(self.root, "alibaba/AliSQL", "AliSQL-8.0.44-3",
                                  "songhuaxiong/alisql", "p1p1bear/alisql-docker", **kwargs)

    def test_latest_changelog_and_packages(self):
        self.add_changelog("2026-06-30", "Old features\n")
        self.add_changelog("2026-10-16", "## BRR\nNew features\n")
        result = self.render()
        self.assertIn("### BRR", result)
        self.assertNotIn("Old features", result)
        self.assertNotIn("Navigation", result)
        self.assertIn("alisql-8.0.44-3-linux-glibc2.17-aarch64.tar.xz", result)
        self.assertIn("alisql-8.0.44-3-linux-glibc2.17-x86_64.tar.xz", result)
        self.assertIn("2026-10-16-zh.md", result)

    def test_older_tag_uses_its_own_changelog(self):
        self.add_changelog("2026-06-30", "Previous release\n")
        self.assertIn("2026-06-30-en.md", self.render())

    def test_links_and_fences(self):
        self.add_changelog("2026-10-16", "## Details\n[Guide](../feature.md#feature)\n"
                           "[External](https://example.com/path)\n```sql\n## unchanged\n"
                           "[Literal](missing.md)\n```\n")
        result = self.render(link_ref="abc123")
        self.assertIn("https://github.com/alibaba/AliSQL/blob/abc123/wiki/feature.md#feature", result)
        self.assertIn("[External](https://example.com/path)", result)
        self.assertIn("```sql\n## unchanged\n[Literal](missing.md)\n```", result)

    def test_missing_changelog_rejected(self):
        with self.assertRaises(ValueError):
            self.render()

    def test_missing_translation_rejected(self):
        path = self.add_changelog("2026-10-16", "Features\n")
        path.with_name(path.name.replace("-en.md", "-zh.md")).unlink()
        with self.assertRaises(ValueError):
            self.render()

    def test_invalid_local_link_rejected(self):
        self.add_changelog("2026-10-16", "[Missing](../missing.md)\n")
        with self.assertRaises(ValueError):
            self.render()

    def test_tag_validation_and_other_series(self):
        for tag in ("master", "AliSQL-8.4.0-1", "AliSQL-8.0.44-3/other", "AliSQL-8.0.44-3\n"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                NOTES.parse_tag(tag)
        result = NOTES.render_notes(self.root, "alibaba/AliSQL", "AliSQL-5.7.44-1",
                                    "songhuaxiong/alisql", "p1p1bear/alisql-docker")
        self.assertIn("alisql-5.7.44-1-linux-glibc2.17-x86_64.tar.xz", result)


if __name__ == "__main__":
    unittest.main()
