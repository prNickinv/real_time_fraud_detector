import json
import os
import pandas as pd
import sys
import time
import logging

from datetime import datetime

from confluent_kafka import Consumer, Producer, KafkaError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.preprocessing import run_preproc
from src.scorer import make_pred

from config import MODEL_PATH
from config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC, SCORING_TOPIC

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self):
        logger.info('Initializing ProcessingService...')
        
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'fraud-scorer',
            'auto.offset.reset': 'earliest'
        }
        self.producer_config = {
             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
             }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)
        
        self.model_path = MODEL_PATH

        logger.info('Service initialized')
        
        
    def process_messages(self):
        """Process messages from Kafka transactions topic"""
        logger.info('Starting to process messages from Kafka...')
        
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # Deserialize JSON
                data = json.loads(msg.value().decode('utf-8'))
                
                # Extract transaction ID and data
                transaction_id = data['transaction_id']
                input_df = pd.DataFrame([data['data']])
                
                logger.info(f'Processing transaction: {transaction_id}')
                processed_df = run_preproc(input_df)
                
                logger.info('Making predictions...')
                submission = make_pred(processed_df, self.model_path)
                
                # Prepare result for Kafka
                result = {
                    'transaction_id': transaction_id,
                    'score': float(submission['score'].iloc[0]),
                    'fraud_flag': int(submission['fraud_flag'].iloc[0])
                }
                
                logger.info(f'Sending result for transaction {transaction_id} to Kafka topic...')
                self.producer.produce(
                    SCORING_TOPIC,
                    value=json.dumps(result)
                )
                self.producer.flush()
                
                logger.info(f'Transaction {transaction_id} processed and result sent to Kafka')
            except Exception as e:
                logger.error(f'Error processing message: {e}', exc_info=True)
            

if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
        #service.consumer.close()
        #service.producer.flush()
    except Exception as e:
        logger.error(f'Service encountered an error: {e}', exc_info=True)
