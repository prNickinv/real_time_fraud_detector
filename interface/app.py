import json
import os
import uuid
import time

import psycopg2

import pandas as pd
import plotly.express as px
import streamlit as st


from datetime import datetime
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, TRANSACTIONS_TOPIC
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD


def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None


def load_file(uploaded_file):
    """Load CSV file into DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    """Send data to Kafka with unique transaction IDs"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Generate unique IDs for all transactions
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        total_rows = len(df)
        
        sent_count = 0
        for idx, row in df.iterrows():
            producer.send(
                topic,
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            sent_count += 1
            progress_bar.progress((idx + 1) / total_rows)
            status_text.text(f"Sent {sent_count}/{total_rows} transactions...")
            time.sleep(0.01)
        
        producer.flush()
        # producer.close()
        
        return True
    except Exception as e:
        st.error(f"Error sending data: {str(e)}")
        return False
    

def get_fraud_transactions(limit=10):
    """Get last N transactions with fraud_flag == 1"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        query = """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM fraud_results
            WHERE fraud_flag = 1
            ORDER BY created_at DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        return df
    
    except Exception as e:
        st.error(f"Error querying database: {e}")
        if conn:
            conn.close()
        return None


def get_score_distribution(limit=100):
    """Get score distribution for last N transactions"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    try:
        query = """
            SELECT score, fraud_flag
            FROM fraud_results
            ORDER BY created_at DESC
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error querying database: {e}")
        if conn:
            conn.close()
        return None


# UI setup    
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

st.title("🚨 Fraud Detection System")

# Create tabs for upload and results
tab1, tab2 = st.tabs(["📤 Upload & Score", "📊 Results"])

with tab1:
    st.header("Upload Transaction Data to Kafka")
    
    uploaded_file = st.file_uploader(
        "Upload CSV file with transactions",
        type=["csv"]
    )
    
    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Add file to state
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Loaded",
            "df": load_file(uploaded_file)
        }
        
        if st.session_state.uploaded_files[uploaded_file.name]["df"] is not None:
            st.success(f"File {uploaded_file.name} uploaded successfully!")
            # Display dataframe
            st.dataframe(st.session_state.uploaded_files[uploaded_file.name]["df"].head())

    # Work on uploaded files
    if st.session_state.uploaded_files:
        st.subheader("📁 Uploaded Files")
        
        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])
            
            with cols[0]:
                st.markdown(f"**File:** `{file_name}`")
                st.markdown(f"**Status:** `{file_data['status']}`")
            
            with cols[2]:
                if st.button(f"Send {file_name} for Scoring", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Sending transactions to Kafka..."):
                            success = send_to_kafka(
                                file_data["df"],
                                TRANSACTIONS_TOPIC,
                                KAFKA_BOOTSTRAP_SERVERS
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Sent"
                                st.success(f"File {file_name} sent successfully!")
                                st.rerun()
                    else:
                        st.error("File does not contain data")
                        

with tab2:
    st.header("Fraud Detection Results")
    
    if st.button("Show Results", type="primary"):
        # Get fraud transactions
        with st.spinner("Loading fraud transactions..."):
            fraud_df = get_fraud_transactions(limit=10)
            
        if fraud_df is not None and len(fraud_df) > 0:
            st.subheader("🔴 Recent Fraudulent Transactions")
            st.dataframe(
                fraud_df,
                column_config={
                    "transaction_id": "Transaction ID",
                    "score": "Score",
                    "fraud_flag": "Fraud Flag",
                    "created_at": "Created At"
                },
                use_container_width=True
            )
        else:
            st.info("No fraudulent transactions found in the database.")
        
        # Get score distribution
        with st.spinner("Loading score distribution..."):
            score_df = get_score_distribution(limit=100)
            
        if score_df is not None and len(score_df) > 0:
            st.subheader(f"📊 Score Distribution (Last {len(score_df)} Transactions)")
    
            # Create histogram with Plotly
            fig = px.histogram(
                score_df,
                x="score",
                nbins=30,
                labels={"score": "Fraud Score", "count": "Count"},
                title="Distribution of Fraud Scores"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No scoring results available.")
