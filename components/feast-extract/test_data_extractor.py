import unittest
from DataExtractor import DataExtractor
import os

class DataExtractorTest(unittest.TestCase):

    def test_retrieve_feast(self):
        self.assertEqual(True, True)

    def test_write_config(self):
        dt_ex = DataExtractor({"project": "clicks_ad_repo", "registry" : "s3://deploy-mlops/registry.db"})
        repo_path = dt_ex.repo_path
        self.assertTrue(os.path.exists(repo_path / "feature_store.yaml"))   
        os.remove(repo_path / "feature_store.yaml")
        self.assertFalse(os.path.exists(repo_path / "feature_store.yaml"))


if __name__ == "__main__":
    unittest.main()