import logging
import numpy as np
import pandas as pd

from catboost import CatBoostClassifier

from config import MODEL_THRESHOLD

logger = logging.getLogger(__name__)


def make_pred(processed_df: pd.DataFrame, model_path):
    """Makes predictions on the preprocessed data."""
    logger.info('Making predictions...')
    
    model = CatBoostClassifier()
    model.load_model(model_path)

    scores = model.predict_proba(processed_df)[:, 1]
    
    submission = pd.DataFrame({
        'score': scores,
        'fraud_flag': np.where(scores >= MODEL_THRESHOLD, 1, 0)
    })
    logger.info('Predictions completed')
    
    return submission
