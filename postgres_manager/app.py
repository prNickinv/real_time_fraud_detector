import json
import logging
import os
import sys

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

from config import KAFKA_BOOTSTRAP_SERVERS, SCORING_TOPIC
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/postgres_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PostgresManager:
    def __init__(self):
        # Kafka configuration
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'postgres-manager',
            'auto.offset.reset': 'earliest'
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([SCORING_TOPIC])
        
        # PostgreSQL configuration
        self.conn = None
        self._connect_db()
        self._create_table()

    def _connect_db(self):
        try:
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info("Connected to PostgreSQL database.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def _create_table(self):
        """Create transaction data table if it doesn't exist"""
        try:
            with self.conn.cursor() as cur:
                # Also add created_at column for sorted retrieval
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS fraud_results (
                        transaction_id VARCHAR(255) PRIMARY KEY,
                        score NUMERIC(10, 8) NOT NULL,
                        fraud_flag INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                self.conn.commit()
                logger.info('Transaction table successfully created in PostgreSQL.')
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    
    def write_to_db(self, transaction_id, score, fraud_flag):
        """Insert transaction result into PostgreSQL"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fraud_results (transaction_id, score, fraud_flag)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO UPDATE 
                    SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag, created_at = CURRENT_TIMESTAMP;
                """, (transaction_id, score, fraud_flag))
                
                self.conn.commit()
                logger.info(f"Inserted/Updated transaction {transaction_id}.")
        except Exception as e:
            logger.error(f"Failed to insert/update transaction {transaction_id}: {e}")
            self.conn.rollback()
            
            
    def process_messages(self):
        """Process messages from Kafka scoring topic"""
        logger.info('Starting to process messages from scoring topic...')
        
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                transaction_id = data['transaction_id']
                score = float(data['score'])
                fraud_flag = int(data['fraud_flag'])
                
                self.write_to_db(transaction_id, score, fraud_flag)

                logger.info(f'Processed transaction {transaction_id} from Kafka scoring topic.')

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                
 
if __name__ == '__main__':              
    logger.info('Starting PostgreSQL manager service...')
    
    manager = PostgresManager()
    try:
        manager.process_messages()
        
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
        if manager.conn:
            manager.conn.close()
        manager.consumer.close()
        
    except Exception as e:
        logger.error(f'PostgreSQL manager service encountered an error: {e}', exc_info=True)
        if manager.conn:
            manager.conn.close()
        manager.consumer.close()
