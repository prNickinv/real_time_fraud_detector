import logging
import numpy as np
import pandas as pd

from geopy.distance import great_circle
from sklearn.impute import SimpleImputer

from config import FEATURES, CAT_FEATURES, CONT_FEATURES

logger = logging.getLogger(__name__)
RANDOM_STATE = 42


def extract_time_features(df):
    """Extracts time-based features from the transaction_time column."""
    logger.info('Extracting time-based features...')
    
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    df['hour'] = df['transaction_time'].dt.hour
    df['year'] = df['transaction_time'].dt.year
    df['month'] = df['transaction_time'].dt.month
    df['day_of_month'] = df['transaction_time'].dt.day
    df['day_of_week'] = df['transaction_time'].dt.dayofweek
    
    return df

def calculate_distance(df):
    """Calculates the distance between customer and merchant."""
    logger.info('Calculating the distance...')
    
    df['distance'] = df.apply(
        lambda x: great_circle(
            (x['lat'], x['lon']), 
            (x['merchant_lat'], x['merchant_lon'])
        ).km,
        axis=1
    )
    
    return df

def run_preproc(input_df):
    """Runs the preprocessing pipeline."""
    logger.info('Preprocessing input data...')
    
    df = input_df.copy()
    df = extract_time_features(df)
    df = calculate_distance(df)

    # Impute categorical features with the most frequent value
    cat_imputer = SimpleImputer(strategy='most_frequent')
    df[CAT_FEATURES] = cat_imputer.fit_transform(df[CAT_FEATURES])
    
    # Impute continuous features with the mean
    cont_imputer = SimpleImputer(strategy='mean')
    df[CONT_FEATURES] = cont_imputer.fit_transform(df[CONT_FEATURES])
    
    return df[FEATURES]
