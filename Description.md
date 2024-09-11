# Pinterest Data Pipeline

Welcome to the Pinterest Data Pipeline project repository! This project aims to provide a robust, scalable, and reliable pipeline to extract, process, and analyze Pinterest data using modern data engineering tools and best practices. The pipeline integrates multiple AWS services and open-source technologies like Kafka, Databricks, AWS Managed Airflow (MWAA), and Kinesis to build a comprehensive data processing system for both batch and real-time analytics.

## Key Features

- **AWS Integration**: Use of core AWS services like EC2, MSK (Managed Streaming for Apache Kafka), S3, and Kinesis to build the data pipeline.
- **Data Streaming with Kafka**: Stream data through Kafka topics to ensure scalable and efficient data transfer.
- **S3 Data Lake**: Store raw data in an Amazon S3 bucket for long-term storage and future processing.
- **Batch Data Processing**: Process batch data using Databricks and PySpark.
- **Workflow Orchestration**: Use AWS MWAA (Managed Workflow for Apache Airflow) to orchestrate batch processing and monitor data workflows.
- **Real-time Stream Processing**: Handle real-time data processing with AWS Kinesis integrated into the pipeline.
- **Custom API**: A RESTful API integrated with Kafka to send data into the pipeline.
  
## Use Cases

This pipeline can be used for:
- Collecting and processing Pinterest-like data from various sources.
- Performing real-time analytics on streaming data.
- Batch processing and transformation of large datasets.
- Creating scalable data architecture for handling growing data needs.
  
## Technologies Used

- **AWS EC2**: For deploying and managing Kafka clusters.
- **Kafka**: For message streaming and data transport.
- **AWS S3**: Data lake for storing raw and processed data.
- **Databricks**: For batch data processing and analysis using PySpark.
- **AWS MWAA**: Workflow orchestration and monitoring for data processing tasks.
- **AWS Kinesis**: For real-time data stream processing.
- **REST API**: Custom API to push data into Kafka topics.

## Getting Started

To get started, follow the instructions in the repository’s [README.md](README.md) for setting up the environment, configuring AWS services, and running the data pipeline.

## Project Structure

```plaintext
Pinterest-Data-Pipeline/
├── api/                          # API to post data to Kafka
├── connectors/                   # Kafka connectors (e.g., for S3)
├── dags/                         # Airflow DAGs for batch processing
├── scripts/                      # Data pipeline scripts
├── notebooks/                    # Databricks notebooks for data analysis
├── resources/                    # Project-related assets (e.g., architecture diagram)
├── .gitignore                    # Files to ignore in Git
├── README.md                     # Main documentation for setting up and running the project
├── LICENSE                       # License for the project
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to contribute to this project by checking the [Contributing Guidelines](CONTRIBUTING.md), and raise any issues you encounter along the way. Happy coding!