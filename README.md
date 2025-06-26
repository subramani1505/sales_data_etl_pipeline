# sales_data_etl_pipeline
A Python ETL pipeline for cleaning and summarizing sales data using pandas.

# Sales Data ETL Micro-Pipeline
## Project Overview

This project demonstrates a foundational Extract, Transform, Load (ETL) pipeline implemented in Python. It processes raw sales transaction data from a CSV file, performs essential data cleaning and transformation, and then loads the cleaned data into a new CSV file, while also generating a summarized view (total sales per category) into a JSON file.

This serves as basic hands-on project to apply core data engineering concepts, including robust file handling, data manipulation with `pandas`, and basic data quality checks.

## Key Features
* **Data Extraction (E):** Reads raw sales data from `sales_data.csv`.
* **Data Transformation (T):**
    * Converts `price` and `quantity` columns to numeric types, handling non-numeric entries gracefully.
    * Calculates `total_sale` for each transaction.
    * Standardizes `category` names (removes whitespace, converts to lowercase).
    * Filters out incomplete or invalid records (e.g., sales with non-numeric prices, or `total_sale` less than 100).
    * Handles missing `customer_email` by filling with 'unknown'.
* **Data Loading (L):**
    * Saves the fully cleaned and transformed sales data to `cleaned_sales_data.csv`.
    * Generates and saves a sales summary (total revenue by category) to `category_sales_summary.json`.
* **Logging:** Implements Python's `logging` module for better visibility into pipeline execution, warnings, and errors.
* **Modularity:** Organized into distinct functions for clarity and reusability.

## Technologies Used

* **Python 3.x**
* **Pandas:** For powerful data manipulation and analysis.
* **Standard Library:** `csv`, `os`, `json`, `logging`.

## How to Run

1.  **Prerequisites:**
    * Ensure Python 3.x is installed on your system.
    * Install the required libraries:
        ```bash
        pip install -r requirements.txt
        ```
        (or `pip install pandas`)

2.  **Download Project Files:**
    * Clone this repository:
        ```bash
        git clone [https://github.com/YourGitHubUsername/sales-data-etl-pipeline.git](https://github.com/YourGitHubUsername/sales-data-etl-pipeline.git)
        cd sales-data-etl-pipeline
        ```
    * Place the `sales_data.csv` (input file) in the same directory as `sales_pipeline.py`.

3.  **Execute the Pipeline:**
    ```bash
    python sales_pipeline.py
    ```

## Output

Upon successful execution, the script will generate two files in the project directory:

* `cleaned_sales_data.csv`: Contains the refined sales transactions.
* `category_sales_summary.json`: Provides a JSON summary of total sales grouped by product category.

You will also see detailed execution logs in your terminal.

## Next Steps / Future Enhancements

* Implement more sophisticated data validation (e.g., regex for email, value ranges).
* Add command-line arguments for input/output file paths.
* Integrate with a proper database (e.g., PostgreSQL) for data loading.
* Explore using Parquet format for output for better performance and compression.
* Containerize the application using Docker.
* Integrate with an orchestration tool like Apache Airflow for scheduled runs.

---

**Connect with me:**
* [www.linkedin.com/in/subramanilvy]

---
