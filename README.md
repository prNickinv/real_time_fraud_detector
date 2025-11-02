# Real-Time Fraud Detection System

by Nikita Artamonov

This project implements a real-time fraud detection system using a machine learning model. The system is built with a microservices architecture, orchestrated with Docker Compose, and utilizes Kafka for messaging, a CatBoost model for fraud scoring, PostgreSQL for storing results, and a Streamlit interface for user interaction and visualization.

## Table of Contents
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Installation](#installation)
- [How to Use](#how-to-use)
  - [Uploading Data](#uploading-data)
  - [Viewing Results](#viewing-results)


## Architecture

1.  **Interface (Streamlit)**: A web-based user interface for uploading transaction data. When a file is uploaded, the data is sent to the `transactions` Kafka topic. Also provides the ability to view results of fraud detection and score distributions visualizations.
2.  **Kafka**: It contains two main topics:
    *   `transactions`: New, unscored transactions are published here.
    *   `scoring`: The fraud detector publishes the scoring results to this topic.
3.  **Fraud Detector**: A Python service that consumes messages from the `transactions` topic. It preprocesses the transaction data, uses a CatBoost model to predict the probability of fraud, and publishes the transaction ID, fraud score, and a fraud flag to the `scoring` topic.
4.  **Postgres Manager**: This service consumes messages from the `scoring` topic and writes the fraud detection results to a PostgreSQL database.
5.  **PostgreSQL**: A relational database used to store the results of the fraud detection.
6.  **Kafka UI**: A web interface for viewing and managing the Kafka cluster and topics.

**NB!** This repository does not contain the training process for the CatBoost model. You can find the training code in this [repository](https://github.com/prNickinv/fraud_detector/blob/main/training/training.ipynb).

## Project Structure

```
real_time_fraud_detector/
├── README.md
├── docker-compose.yaml
├── .gitignore
├── fraud_detector/
│   ├── app/
│   │   └── app.py
│   ├── models/
│   │   └── fraud_detection_model.cbm
│   ├── src/
│   │   ├── preprocessing.py
│   │   └── scorer.py
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── interface/
│   ├── .streamlit/
│   │   └── config.toml
│   ├── app.py
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── postgres_manager/
│   ├── app.py
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
└── test_data/
    ├── test_100.csv
    └── test_500.csv
```

## Getting Started

### Installation

1.  **Clone the repository** (or ensure you are in the project's root directory).

    ```bash
    git clone https://github.com/prNickinv/real_time_fraud_detector.git
    cd real_time_fraud_detector
    ```

2.  **Build and run the services using Docker Compose:**

    ```bash
    docker-compose up --build
    ```

    This command will build the Docker images for the `fraud_detector`, `postgres_manager`, and `interface` services and then start all the services defined in the `docker-compose.yaml` file.

3.  **Access the services:**
    -   **Streamlit UI**: Open your web browser and go to `http://localhost:8501`.
    -   **Kafka UI**: To inspect the Kafka topics and messages, navigate to `http://localhost:8080`.

## How to Use

### Uploading Data

1.  Navigate to the Streamlit interface at `http://localhost:8501`.
2.  In the "📤 Upload & Score" tab, click on "Browse files" to select a CSV file. You can use the files provided in the `test_data` directory.
3.  Once the file is uploaded, you will see a preview of the data.
4.  Click the "Send [file_name] for Scoring" button to send the transactions to the Kafka `transactions` topic for processing.

### Viewing Results

1.  After sending the data, switch to the "📊 Results" tab in the Streamlit interface.
2.  Click the "Show Results" button.
3.  The interface will display:
    -   **Recent Fraudulent Transactions**: A table showing the latest transactions that were flagged as fraud.
    -   **Score Distribution**: A histogram visualizing the distribution of fraud scores for the most recent transactions.
