# Yahoo Finance ELT with Kafka

This project uses Kafka to create a real-time data pipeline for Yahoo Finance data. A Python producer fetches financial data for a predefined list of stock symbols from the Yahoo Finance API (via RapidAPI) and sends it to a Kafka topic.

## Project Structure

```
project-02_yahoo_elt/
├── docker-compose.yaml
├── .env
├── producer/
│   ├── main.py
│   ├── Dockerfile
│   └── requirements.txt
└── README.md
```

## Prerequisites

- Docker and Docker Compose
- A RapidAPI account and an API key for the [YFinance API](https://rapidapi.com/davethebeast/api/yahoo-finance166).

## Setup and Execution

1.  **Clone the repository (if you haven't already).**

2.  **Configure your RapidAPI Key:**
    -   Rename the `.env.example` file to `.env`.
    -   Open the `.env` file and replace `your_rapidapi_key_here` with your actual RapidAPI key.

3.  **Run the pipeline:**
    Open a terminal in the `project-02_yahoo_elt` directory and run:
    ```bash
    docker-compose up --build
    ```
    This command will:
    -   Build the Docker image for the producer.
    -   Start the Zookeeper and Kafka containers.
    -   Start the producer container, which will begin fetching data and sending it to the `yahoo_finance_data` Kafka topic.

4.  **Verify the data stream (optional):**
    You can inspect the Kafka topic to see the data being produced.
    -   Open a new terminal and exec into the Kafka container:
        ```bash
        docker-compose exec kafka /bin/bash
        ```
    -   Inside the container, run the Kafka console consumer:
        ```bash
        kafka-console-consumer --bootstrap-server localhost:9092 --topic yahoo_finance_data --from-beginning
        ```
    You should see a stream of JSON objects containing the financial data for the configured stock symbols.

5.  **Stopping the pipeline:**
    To stop the containers, press `Ctrl+C` in the terminal where `docker-compose up` is running, and then run:
    ```bash
    docker-compose down
    ```
