# ThreadedCSVSync

## Overview
ThreadedCSVSync is a Java-based project that leverages the power of multithreading and concurrency to efficiently read data from CSV files and synchronize it with a MySQL database. 
The project consists of two main components:
1. **CSV_DB**: Reads data from a CSV file and inserts it into the database using multithreading and batch insertion.
2. **DBDataToCSV**: Extracts data from the database and writes it back to a CSV file using concurrent processing.

## Technologies Used
- **Java**: The primary programming language used for developing the application.
- **MySQL**: The relational database management system used for data storage.
- **OpenCSV**: A library used for reading and writing CSV files.
- **Java Concurrency Utilities**: Utilized for managing multithreading and task execution.

## Features
- **Multithreading**: Efficiently processes large datasets by dividing the workload across multiple threads.
- **Batch Processing**: Reduces the number of database transactions by inserting records in batches.
- **Progress Monitoring**: Provides feedback on the number of records processed and elapsed time.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Mchiir/ThreadedCSVSync.git
