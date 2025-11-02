import os
from pathlib import Path


# Features
FEATURES = ['merch', 'cat_id', 'gender', 'one_city', 'us_state', 'jobs', 
            'hour', 'year', 'month', 'day_of_month', 'day_of_week', 
            'amount', 'population_city', 'distance']

CAT_FEATURES = ['merch', 'cat_id', 'gender', 'one_city', 'us_state', 'jobs', 
                'hour', 'year', 'month', 'day_of_month', 'day_of_week']

CONT_FEATURES = ['amount', 'population_city', 'distance']

# Threshold for scoring
MODEL_THRESHOLD = os.getenv('MODEL_THRESHOLD', 0.98)

# Paths
ROOT_PATH = Path(__file__).resolve().parent
MODEL_PATH = os.getenv('MODEL_PATH', os.path.join(ROOT_PATH, 'models', 'fraud_detection_model.cbm'))

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
SCORING_TOPIC = os.getenv('KAFKA_SCORING_TOPIC', 'scoring')
