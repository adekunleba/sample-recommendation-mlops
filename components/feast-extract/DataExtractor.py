from feast import FeatureStore, feature_store

class DataExtractor:
    """
    Connect to feast - retrieve the important feature for training.

    For training remember to retrieve features from the offline store.
    Also note that there is no feature engineering that should happen here, we are only meant
    to retrieve an existing features and run the pipeline with it straight off.
    """

    def __init__(self, store: FeatureStore) -> None:
        self.store = store


    def get_training_sets(feature_refs, entity_df, get_as_df=False):
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

    