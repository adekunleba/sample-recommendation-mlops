import unittest
from data_extractor import DataExtractor
import os

class DataExtractorTest(unittest.TestCase):

    def test_retrieve_feast(self):
        self.assertEqual(True, True)

    def test_write_config(self):
        dt_ex = DataExtractor({"project": "clicks_ad_repo", "data_path" : "/tmp/feast_data"})
        repo_path = dt_ex.repo_path
        self.assertTrue(os.path.exists(repo_path / "feature_store.yaml"))   
        os.remove(repo_path / "feature_store.yaml")
        self.assertFalse(os.path.exists(repo_path / "feature_store.yaml"))

    def test_retrieve_dbs(self):
        os.environ["MINIO_SERVER_ENDPOINT"] =  "http://localhost:8000"
        os.environ["ACESS_KEY_ID"] = "minio"
        os.environ["SECRET_ACCESSKEY"]  = "minio123"

        dt_ex = DataExtractor({"project": "clicks_ad_repo", "data_path" : "/tmp/feast_data"})
        self.assertTrue(dt_ex.setup())
        import shutil
        shutil.rmtree('/tmp/feast_data', ignore_errors=True)
        os.remove(dt_ex.repo_path / "feature_store.yaml")

        
if __name__ == "__main__":
    unittest.main()