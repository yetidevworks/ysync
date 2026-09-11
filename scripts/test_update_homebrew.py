"""Release metadata must agree before an installable formula is emitted."""
import copy
import unittest

from update_homebrew import REPO, TARGETS, render


class HomebrewReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tag = "v0.1.0"
        self.release = {"tagName": self.tag, "isDraft": False, "assets": []}
        checksums = []
        for index, target in enumerate(TARGETS):
            digest = f"{index + 1:064x}"
            name = f"ysync-{self.tag}-{target}.tar.gz"
            checksums.append(f"{digest}  ./{name}")
            self.release["assets"].append({
                "name": name, "url": f"https://github.com/{REPO}/releases/download/{self.tag}/{name}",
                "state": "uploaded", "digest": f"sha256:{digest}",
            })
        self.checksums = "\n".join(checksums)

    def test_all_platforms_use_verified_hashes(self):
        formula = render(self.tag, self.release, self.checksums)
        self.assertNotIn("@", formula)
        for asset in self.release["assets"]:
            self.assertIn(asset["url"], formula)
            self.assertIn(asset["digest"].removeprefix("sha256:"), formula)

    def test_rejects_unpublished_or_inconsistent_metadata(self):
        mutations = [
            lambda r: r.update(isDraft=True),
            lambda r: r.update(tagName="v0.2.0"),
            lambda r: r["assets"][0].update(digest="sha256:" + "0" * 64),
            lambda r: r["assets"][0].update(url="https://example.com/other.tar.gz"),
            lambda r: r["assets"][0].update(state="new"),
            lambda r: r["assets"].pop(),
        ]
        for mutation in mutations:
            release = copy.deepcopy(self.release)
            mutation(release)
            with self.subTest(release=release), self.assertRaises((ValueError, KeyError)):
                render(self.tag, release, self.checksums)

    def test_rejects_missing_or_duplicate_checksums(self):
        for checksums in (self.checksums.splitlines()[0], self.checksums + "\n" + self.checksums):
            with self.assertRaises((ValueError, KeyError)):
                render(self.tag, self.release, checksums)

    def test_rejects_unsafe_tag(self):
        with self.assertRaises(ValueError):
            render("v0.1.0/../../main", self.release, self.checksums)


if __name__ == "__main__":
    unittest.main()
