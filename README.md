# Massive Event Streamer

A high-performance asynchronous data pipeline for real-time cryptocurrency market monitoring. The system leverages **Python (Asyncio)** to stream live data through **Apache Kafka**, which is then processed and stored in **TimescaleDB** for time-series optimization. 

The entire infrastructure is containerized using **Docker** and orchestrated with **Docker Compose**, featuring a pre-configured **Grafana** dashboard for instant visualization. A centralized **Nginx** gateway acts as a reverse proxy to ensure secure and streamlined access to the monitoring services.
