from argparse import ArgumentParser
from feast import FeatureStore
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd

class DataExtractor:
    """
    Connect to feast - retrieve the important feature for training.

    For training remember to retrieve features from the offline store.
    Also note that there is no feature engineering that should happen here, we are only meant
    to retrieve an existing features and run the pipeline with it straight off.
    """

    def __init__(self,  config: dict, store: FeatureStore = None) -> None:
        self.store = store
        self.repo_path = Path('.')
        self.feature_yaml :Path = self.repo_path / "feature_store.yaml"
        self.config = config
        _ = self.write_config()
    

    def write_config(self):
        if self.store is None:
            self.feature_yaml.write_text(
                dedent(
                    f"""
                project: {self.config['project']}
                registry: {self.config['registry']}
                provider: redis
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
        historical_data = self.store.get_historical_features(
            feature_refs=feature_refs,
            entity_df= entity_df
        )
        if get_as_df:
            return historical_data.to_df()

        return historical_data

def main(args):
    """
    Parse the Paths to the csv as output to the pipeline.
    """
    dt_ex = DataExtractor({"project": "clicks_ad_repo", "registry" : "s3://deploy-mlops/registry.db"})
    # We can keep populating the entity_df and feature_refs for training.
    entity_df = pd.DataFrame.from_dict({
        "session_id": [218564],
        "event_timestamp" : datetime(2018, 10, 15, 8, 58, 00),
    })
    feature_refs=[
            "view_log_table:device_type",
            "view_log_table:item_id"
        ]
    assert os.environ.get("FEAST_S3_ENDPOINT_URL") is not None, "Feast url endpoint should be set with export"
    assert os.environ.get("AWS_ACCESS_KEY_ID") is not None, "AWS access key id should be set with export from cmd line"
    assert os.environ.get("AWS_SECRET_ACCESS_KEY") is not None, "AWS Secret key should be set with export form cmd line"
    # Validate that environment variables needed are available

    feature_df = dt_ex.get_training_sets(feature_refs=feature_refs, entity_df=entity_df, get_as_df=True)
   
    # dataframe csv_path
    Path(args.output_csv_path_file).parent.mkdir(parents=True, exist_ok=True)
    # This is supposed to be raw features hence should not contain headers and index
    feature_df.to_csv(args.output_csv_path_file, header=False, index=False)



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