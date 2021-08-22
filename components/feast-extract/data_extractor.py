from argparse import ArgumentParser
import boto3
from botocore.config import Config
from feast import FeatureStore
import os
from pathlib import Path
from textwrap import dedent
from datetime import datetime

import pandas as pd

class DataExtractorSetupException(Exception):
    
    def __init__(self, *args: object) -> None:
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self) -> str:
        if self.message:
            return f'DataExtractorSetupException, {self.message}'
        else:
            return 'DataExtractorSetupException:'

class DataExtractor:
    """
    Connect to feast - retrieve the important feature for training.

    For training remember to retrieve features from the offline store.
    Also note that there is no feature engineering that should happen here, we are only meant
    to retrieve an existing features and run the pipeline with it straight off.
    """

    def __init__(self,  config: dict, store: FeatureStore = None) -> None:
        self.store = store
        self.config = config
        self.data_path = self.config.get('data_path', '/tmp/data')
        self.repo_path = Path('.')
        self.feature_yaml :Path = self.repo_path / "feature_store.yaml"
        _ = self.write_config()
        self.is_setup = self.setup()
    

    def write_config(self):
        if self.store is None:
            self.feature_yaml.write_text(
                dedent(
                    f"""
            project: {self.config['project']}
            registry: {str(Path(self.data_path) / 'registry.db')}
            provider: local
            online_store:
                path: {str(Path(self.data_path) / 'online_store.db')}
                """
                )
            )
        self.store = FeatureStore(self.repo_path)


    def get_training_sets(self, feature_refs, entity_df, get_as_df=False):
        """
        Entity df is the list of elements to retrieve as a pandas dataframe

        Params:
        ------
        - feature_refs: list of feature to retrieve
            E.g 
            [
                feature_table:feature_name_1,
                feature_table:feature_name_2
                .....
                feature_table:feature_name_n
            ]
        - entity_df - feature points to retrieve e.g customer ids and timestamps.
        """
        if self.is_setup:
            historical_data = self.store.get_historical_features(
                feature_refs=feature_refs,
                entity_df= entity_df
            )
            if get_as_df:
                return historical_data.to_df()

            return historical_data
        else:
            raise DataExtractorSetupException("Setup is not completed, check retrieving online and offline stores")
    
    def connect_s3(self):
        """Should connect """
        s3 = boto3.resource('s3',
        # Should probably check to ensure that the various env variables are given.
            endpoint_url = os.environ.get('MINIO_SERVER_ENDPOINT'),
            aws_access_key_id=os.environ.get('ACESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('SECRET_ACCESSKEY'),
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        return s3

    def retrieve_feature_db(self, s3_client, bucket_name, file_name, download_location):
        """Should retrieve feature db to a local file directory in the container"""
        try:
            os.makedirs(download_location, exist_ok=True)
        except FileExistsError as err:
            logging.info("File already existed hence parsing")
            pass
        return s3_client.Bucket(bucket_name).download_file(file_name, os.path.join(download_location, file_name))


    def setup(self):
        """Setup should prepare the feature store repo by getting the dbs from s3
        """

        # Retrive online and offline.dbs
        # Create the yaml file with this path.

        # Connect to s3 
        s3_client = self.connect_s3()

        # Retrieve offline db
        data_path = self.data_path
        bucket_name = 'deploy-mlops'
        self.retrieve_feature_db(
            s3_client=s3_client,
            bucket_name=bucket_name,
            file_name='registry.db',
            download_location=data_path
        )

        # Retrieve online db
        self.retrieve_feature_db(
            s3_client=s3_client,
            bucket_name=bucket_name,
            file_name='online_store.db',
            download_location=data_path
        )


        # Retrieve datas too -
        # Apparently, feast requires that the data are accessible in all cases whether in retrieveing features or storing it
        data_bucket_name = 'deploy-mlops-data'
        self.retrieve_feature_db(
            s3_client=s3_client,
            bucket_name=data_bucket_name,
            file_name='train.parquet',
            download_location=data_path
        )

        self.retrieve_feature_db(
            s3_client=s3_client,
            bucket_name=data_bucket_name,
            file_name='view_log.parquet',
            download_location=data_path
        )

        # Create config yaml
        return (
            os.path.exists(os.path.join(self.data_path, 'registry.db'))
            and os.path.exists(os.path.join(self.data_path, 'online_store.db'))
            and os.path.exists(os.path.join(self.repo_path, 'feature_store.yaml'))
        )

    def cleanup(self):
        import shutil
        shutil.rmtree(self.data_path, ignore_errors=True)
        os.remove(os.path.join(self.repo_path, 'feature_store.yaml'))

def main(args):
    """
    Parse the Paths to the csv as output to the pipeline.
    """
    dt_ex = DataExtractor({"project": "click_ad_feast", "data_path" : "/tmp/data"})
    # We can keep populating the entity_df and feature_refs for training.
    entity_df = pd.DataFrame.from_dict({
        "session_id": [218564],
        "event_timestamp" : datetime(2018, 10, 15, 8, 58, 00),
    })
    feature_refs=[
            "view_log_table:device_type",
            "view_log_table:item_id"
        ]
    assert os.environ.get("MINIO_SERVER_ENDPOINT") is not None, "Feast url endpoint should be set with export"
    assert os.environ.get("AWS_ACCESS_KEY_ID") is not None, "AWS access key id should be set with export from cmd line"
    assert os.environ.get("AWS_SECRET_ACCESS_KEY") is not None, "AWS Secret key should be set with export form cmd line"
    # Validate that environment variables needed are available

    feature_df = dt_ex.get_training_sets(feature_refs=feature_refs, entity_df=entity_df, get_as_df=True)
   
    # dataframe csv_path
    Path(args.output_csv_path_file).parent.mkdir(parents=True, exist_ok=True)
    # This is supposed to be raw features hence should not contain headers and index
    feature_df.to_csv(args.output_csv_path_file, header=False, index=False)


    # cleanup
    dt_ex.cleanup()



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--output_csv_path_file", type=str, help="path to text holding feature csv file path")
    args = parser.parse_args()
    main(args=args)
    #We could ddo something like this here - to create the component but is that best practices?
    """
    from kfp.components import InputPath, OutputPath, create_component_from_func

    split_table_into_folds_op = create_component_from_func(
        split_table_into_folds,
        base_image='python:3.7',
        packages_to_install=['scikit-learn==0.23.1', 'pandas==1.0.5'],
        output_component_file='component.yaml',
    )
    """