# Magnificent 7 Market Concentration Analysis Pipeline

## Project Overview
This project implements a cloud-native data pipeline on Amazon Web Services (AWS) that tracks and analyzes the market concentration of the Magnificent 7 technology companies (Apple, Microsoft, Alphabet, Amazon, Nvidia, Meta, and Tesla) relative to the broader S&P 500 index. The pipeline collects historical market data, processes it for analysis, and makes it available for visualization through a series of automated AWS services. The goal is to help investors understand their true portfolio exposure through passive investments in ETFs that track the S&P 500.

## Repository Structure
```plaintext
mags/
├── README.md                               # Project overview and main documentation
├── architecture/                           # Architecture diagrams
│   ├── architecture.mermaid                # Architecture diagram in Mermaid format
│   └── architecture.png                    # Architecture diagram image
├── data/                                   # Data files for S&P 500 constituent companies
│   ├── IVV_holdings.csv                    # iShares Core S&P 500 ETF (IVV) Detailed Holdings and Analytics
│   └── market_data.json                    # Raw data from raw/magnificent7/trading_date=2024-12-13/
└── services/                               # AWS Services
    ├── athena/                             # Athena queries for data validation and insights
    │   ├── all_magnificent7.sql            # Query to fetch Magnificent 7 data
    │   ├── earliest_latest_date.sql        # Query for earliest and latest trading dates
    │   ├── market_cap_extremes.sql         # Query for market cap extremes
    │   └── min_max_concentration.sql       # Query for concentration metrics
    ├── cloudshell/                         # CloudShell scripts for S&P 500 ticker preparation
    │   ├── filter_equity_tickers.py        # Python script to filter equity tickers
    │   └── output/                         # Output files from CloudShell scripts
    │       ├── filtered_tickers.csv        # Filtered list of equity tickers
    │       └── tickers_array.py            # Python array of filtered tickers
    ├── glue/                               # Glue SQL transformations
    │   ├── company_detail.sql              # Transformation for company details
    │   ├── concentration_metrics.sql       # Transformation for concentration metrics
    │   ├── daily_trading.sql               # Transformation for daily trading data
    │   ├── failed_collections.sql          # Transformation for failed data collections
    │   └── magnificent7_metrics.sql        # Transformation for Magnificent 7 metrics
    ├── lambda/                             # Lambda functions for data collection
    │   ├── magnificent7-historical-data-collector/     # Collector for historical Magnificent 7 data
    │   │   ├── layer/                      # Layer for dependencies
    │   │   │   └── python.zip              # Python dependencies
    │   │   └── lambda_function.py          # Main Lambda function
    │   ├── trading-dates-generator/        # Generator for trading dates
    │   │   └── lambda_function.py          # Main Lambda function
    │   └── trading-dates-generator-range/  # Generator for trading date ranges
    │       └── lambda_function.py          # Main Lambda function
    ├── quicksight/                         # QuickSight visualizations and configurations
    ├── redshift/                           # Redshift data warehouse scripts
    │   ├── copy_jobs.ipynb                 # Jupyter notebook for Redshift copy jobs
    │   ├── schema_definition.sql           # SQL script for schema definitions
    │   ├── table_definitions.ipynb         # Jupyter notebook for table definitions
    └── step-functions/                     # Step Function state machine definitions
        ├── historical-data-collection/     # Historical data collection state machine
        │   └── code.json                   # State machine definition
        └── historical-data-collection-missing-data/  # Missing data collection state machine
            └── code.json                   # State machine definition
```

## Architecture Overview

![Architecture Diagram](architecture/architecture.png)

### Service Architecture
The platform implements a serverless event-driven architecture that collects, processes, and analyzes stock market data through several AWS services. The system is divided into four main components:

1. Storage Layer
   - **Amazon S3 (Data Lake)**
     - Raw (semi-structured) data landing zone for market data
     - Processed (structured) data storage for analytics
     - Partitioned by date and data type
     - Follows a multi-zone architecture (raw, processed, analytics)

2. Data Collection Layer
   - **AWS Lambda**
     - Daily market data collection
     - Historical data backfill capabilities
     - Error handling and retry logic
     - Rate limiting and API quota management
   
   - **AWS Step Functions**
     - Orchestrates data collection workflows
     - Manages state transitions and error handling
     - Coordinates multiple Lambda executions
     - Implements retry and backoff strategies

3. Data Processing Layer
   - **AWS CloudShell**
     - Initial data cleaning and preparation
     - Ticker symbol validation
     - Ad-hoc data manipulation tasks
     - Script execution environment

   - **AWS Glue**
     - ETL job management and scheduling
     - Data catalog maintenance
     - Schema evolution handling
     - Data quality validation

4. Analytics Layer
   - **Amazon Athena**
     - SQL-based data validation
     - Ad-hoc analysis capabilities
     - Data quality checks
     - Performance monitoring queries

   - **Amazon Redshift**
     - Central data warehouse
     - Complex analytical queries
     - Historical trend analysis
     - Performance optimization

   - **Amazon QuickSight**
     - Interactive dashboards
     - Metric visualization
     - Custom analysis views
     - Automated reporting

### Data Flow
1. Collection Process
   - CloudShell prepares API data targets
   - Step Functions trigger Lambda functions on schedule
   - Lambda collects market data from external APIs
   - Raw data stored in S3 landing zone
   - Failed collections logged for retry

2. Processing Pipeline
   - Glue jobs transform raw data into analytical formats
   - Data quality validated through Athena queries
   - Processed data loaded into Redshift

3. Analytics Workflow
   - Redshift powers complex analytical queries
   - QuickSight consumes Glue Data Catalog data for visualizations
   - Custom analyses and dashboards track Magnificent 7 concentration
   - Share insights with data stories

## Prerequisites

### Administrative Access
This project was created in an AWS Root Account with administrative access. Ensure you have administrative access to the services described in the Service Architecture. Resource policies as well as user and service IAM roles may need to be modified for not root users.

### Supported Region
Modern data analytics workflows prioritize rapid insight generation through visualizations. Since Amazon QuickSight is central to our visualization strategy, deploy all resources in a single QuickSight-supported region. This pipeline uses **US West (Oregon) (us-west-2)**.

Reference: [QuickSight Supported Regions](https://docs.aws.amazon.com/quicksight/latest/user/regions-qs.html)

### Polygon.io API Setup
This project requires a Polygon.io Stocks Starter subscription for market data collection. Each Lambda function makes 500+ API calls per trading day, exceeding the Basic tier's 5 calls/minute limit.

#### Setup Steps
1. Create account: [Polygon.io Signup](https://polygon.io/dashboard/signup)
2. Subscribe to Stocks Starter tier
3. Get API key: [Dashboard](https://polygon.io/dashboard/keys)

Save the API key for future use as the `POLYGON_API_KEY` Lambda environment variable.

Example API key format:
```json
_eaBY7FrBg2jiHB2PM63CY7HUgyQLtLf
```

## Collection Process

### S&P 500 Constituent Data Collection
The analysis compares Magnificent 7 market caps against the S&P 500 index. Since S&P Dow Jones Indices data is proprietary, we use BlackRock's IVV ETF holdings (December 12th, 2024) as our constituent data source. While the constituents of the S&P 500 are always changing, we will use this point in time holdings data to simplify our analysis.

#### Data Source
For ETF details and documentation: [IVV ETF Overview](https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf)

Detailed Holdings and Analytics: [IVV Holdings CSV](https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund)  

[IVV_holdings.csv](data/IVV_holdings.csv)
```csv
iShares Core S&P 500 ETF
Fund Holdings as of,"Dec 06, 2024"
Inception Date,"May 15, 2000"
Shares Outstanding,"957,950,000.00"
Stock,"-"
Bond,"-"
Cash,"-"
Other,"-"
 
Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
"AAPL","APPLE INC","Information Technology","Equity","41,700,670,815.44","7.15","41,700,670,815.44","171,720,766.00","242.84","United States","NASDAQ","USD","1.00","USD","-"
"NVDA","NVIDIA CORP","Information Technology","Equity","39,575,685,429.36","6.79","39,575,685,429.36","277,841,094.00","142.44","United States","NASDAQ","USD","1.00","USD","-"
"MSFT","MICROSOFT CORP","Information Technology","Equity","37,238,281,689.56","6.38","37,238,281,689.56","83,951,308.00","443.57","United States","NASDAQ","USD","1.00","USD","-"
"AMZN","AMAZON COM INC","Consumer Discretionary","Equity","23,951,910,419.43","4.11","23,951,910,419.43","105,501,081.00","227.03","United States","NASDAQ","USD","1.00","USD","-"
...
```

### Data Preparation with CloudShell
Use AWS CloudShell to extract equity ticker symbols from `IVV_holdings.csv` using Python. This ensures accurate data handling by removing non-equity holdings and extraneous data through automation. CloudShell provides a pre-configured environment with AWS CLI and Python, eliminating setup requirements and ensuring consistent execution.

#### Steps to run the data cleaning script in AWS CloudShell
1. **Upload the CSV file**
    - Open **AWS CloudShell** in the AWS Management Console.
    - Use the **"Upload File"** option in CloudShell to upload the `IVV_holdings.csv` file into your working directory.

2. **Upload the Python file**
    - Use the **"Upload File"** option in CloudShell to upload the `filter_equity_ticker.py` file into your working directory.

    [filter_equity_ticker.py](services/cloudshell/filter_equity_tickers.py)
    ```python
    import pandas as pd

    # File path for the input CSV
    file_path = 'IVV_holdings.csv'

    # Read the raw content of the file to inspect structure
    with open(file_path, 'r') as file:
        raw_content = file.readlines()

    # Identify the start of actual data (header line)
    header_line_index = next(
        (index for index, line in enumerate(raw_content) if "Ticker,Name," in line), None
    )
    ...
    ```


3. **Install Dependencies**
    - Install `pandas` in the CloudShell environment:
    ```bash
    pip3 install pandas --user
    ```

4. **Run the Script**
    - Run the Python script in CloudShell:
    ```bash
    python3 filter_equity_tickers.py
    ```

5. **Download the Results**
    - After the script runs successfully, you will have two output files:
        - [filtered_ticker.csv](services/cloudshell/output/filtered_tickers.csv)
        - [tickers_array.py](services/cloudshell/output/tickers_array.py)
        ```python
        tickers = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'META', 'TSLA', 'GOOGL', 'BRKB', 'GOOG', 'AVGO', 'JPM', 'LLY', 'V', 'UNH', 'XOM', 'COST', 'MA', 'HD', 'WMT', 'PG', 'NFLX', 'JNJ', 'CRM', 'BAC', 'ABBV', 'ORCL', 'CVX', 'MRK', 'WFC', 'ADBE', 'KO', 'CSCO', 'NOW', 'ACN', 'AMD', 'IBM', 'PEP', 'LIN', 'MCD', 'DIS', 'PM', 'TMO', 'ABT', 'ISRG', 'CAT', 'GE', 'GS',
        ... 
        ```
    - Use the **"Download File"** option in CloudShell to download these files to your local machine.
    
Note: We will use the `tickers_array.py` code in our data collection Lambda function.

### Data Lake Implementation with Amazon S3
S3 bucket needs to store historical stock data, requiring a structure that supports efficient querying and maintains data organization. Here's a detailed walkthrough:

1. S3 Bucket Creation
    - Go to **S3** in the AWS Console. Make sure you are in the QuickSight supported region you choose earlier.
    - Click **"Create bucket"**.
    - For **"Bucket name"** enter "magnificent7-market-data".
    - Enable **"Bucket Versioning"** to safe guard against duplicate files.
    - Click **"Create bucket"**.
2. Bucket directory structure
    - Click on the bucket name you just created from the **"General purpose buckets"** table.
    - Use the **"Create folder"** button to create the following bucket structure:
    ```plaintext
    magnificent7-market-data/
    ├── raw/
    │   └── magnificent7/
    ├── processed/
    │   ├── concentration_metrics/
    │   ├── company_details/
    │   ├── daily_trading/
    │   ├── magnificent7_metrics/
    │   └── failed_collections/
    └── analytics/
        ├── concentration_metrics/
        ├── company_details/
        ├── daily_trading/
        ├── magnificent7_metrics/
        └── failed_collections/
    ```
    - This structure uses partitioning by date, which helps with data organization and query performance.

3. Edit Bucket Policy
    - Click on the Permission tab and scroll down to Bucket policy.
    - Click "Edit" and paste the following policy to allow access from any authenticated service within your AWS account.
    ```json
    {
        "Version": "20-10-17",
        "Statement": [
            {
                "Sid": "AllowAWSServiceAccess",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::ACCOUNT-ID:root"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::magnificent7-market-data",
                    "arn:aws:s3:::magnificent7-market-data/*"
                ],
                "Condition": {
                    "StringEquals": {
                        "aws:PrincipalAccount": "ACCOUNT-ID"
                    }
                }
            }
        ]
    }
    ```


### Historical Data Collection with AWS Lambda

The data collection process is implemented using two AWS Lambda functions: one to collect market data and another to generate trading dates. Let's walk through setting up these functions step by step.

#### Market Data Collector Lambda Function
The Market Data Collector function serves as the core component of our data pipeline, retrieving and processing daily market data from Polygon.io. The function implements a stateless architecture, processing one trading day per execution to ensure reliability and simplify error handling.

The function executes a carefully orchestrated sequence of API calls to gather comprehensive market data. It begins by collecting S&P 500 constituent data, then focuses on the Magnificent 7 companies. To manage API rate limits effectively, the function implements a controlled delay between requests, preventing throttling while maintaining efficient data collection.

[magnificent7-historical-data-collector/lambda_function.py](services/lambda/magnificent7-historical-data-collector/lambda_function.py)
```python
from polygon import RESTClient
import json
import os
import boto3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time

def get_previous_trading_date():
    """
    Determines the most recent trading day, accounting for weekends.
    This function ensures we don't request data for non-trading days.
    
    Returns:
        str: Date in YYYY-MM-DD format representing the last trading day
    """
...
```

The function implements a date-partitioned storage strategy in Amazon S3, organizing data for optimal query performance:
```plaintext
s3://magnificent7-market-data/
└── raw/
    └── magnificent7/
        └── trading_date=YYYY-MM-DD/
            └── market_data.json
```


#### Steps to create historical data collection
1. **Create Lambda function**
    - Go to **Lambda** in the AWS Console.
    - Click **"Create function"**.
    - Choose **"Author from scratch"**.
    - Enter "magnificent7-historical-data-collector" for the **Function name**.
    - Select "Python 3.13" for "Runtime".
    - Leave other configurations as is.
    - Click on **"Create function"**.
2. **Configure Lambda function**
    - After the function is successfully created you will be taken to the function details page. Click on the **"Configuration"** tab below the **"Function overview"**.
    - Within **"General configuration"** click **"Edit**" and add the update the following values:
    ```yaml
    Memory: 512 MB
    Timeout: 15 min 0 seconds (maximum)
    ```
    - Click **"Save"**.
    - Within **"Environment variables"** click **"Edit**". Then click **"Add environment variable"** and add the update the following values:
    ```yaml
    POLYGON_API_KEY: [API Key]
    DATA_BUCKET: [S3 Bucket Name]
    ```
    - Click **"Save"**.
3. Configure Lambda IAM Role
    - Within "Permissions" click the Role name under "Execution role" to open the IAM console.
    - Add these policies:
        - AWSLambdaBasicExecutionRole (should already be there)
        - S3 access policy 
            - Create a custom policy for S3 access, click "Add permission" and select "Create inline policy"
            - Click "JSON" in the Policy editor
            - Paste the following statement:
            ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:PutObject",
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::magnificent7-market-data/*",
                            "arn:aws:s3:::magnificent7-market-data"
                        ]
                    }
                ]
            }
            ```
            - Click "Next" and input a descriptive Policy name like "S3AccessPolicyMag7". We will reuse this policy and add it to all our services needing permissions to access our Magnificent 7 market data.
            - Click "Create policy". The policy wil be created and added to the service role attached to your Lambda function.
4. Creat a Lambda layer
    - Since we're using external libraries, we need to create a Lambda layer to install required dependencies. Go to "CloudShell".
    - Create a new directory on your local machine called "python".
    ```bash
    mkdir python
    ```
    - Run the following script to install the dependencies.
    ```bash
    pip3 install polygon-api-client pandas-market-calendars -t python/
    ```
    - Zip the "python" folder.
    ```bash
    zip -r python.zip python/
    ```
    - Use the **"Download File"** option in CloudShell to download the [python.zip](services/lambda/magnificent7-historical-data-collector/layer/python.zip) file to your local machine.
    - In Lambda console, go to "Layers".
    - Click "Create layer".
    - Name the layer and upload the zipped file.
    - Select "Python 3.13" for the Compatabile runtime.
    - Click "Create".
5. Add the layer to the Lambda function
    - Go to the "magnificent7-historical-data-collector" function details page.
    - Click the "Code" tab and scroll down to Layers.
    - Click "Add a layer".
    - Choose "Custom layers" and select the custom layer you just created.
    - Click "Add".
6. **Add Lambda function code**
    - Click on the **"Code"** tab below the **"Function overview"**.
    - In **"Code source"** add the code from `magnificent7-historical-data-collector/lambda_function.py`
    - Click **"Deploy"** to save the function.

#### Steps to test historical data collection
1. **Create a test event**
    - Next click **"Test"**, then **"Create new test event"**.
    - Add an **"Event Name"** and enter an empty **"Event JSON"**
    ```json
    {}
    ```
    - Click **"Save"**.
    - Run the test by clicking **"Test"** and selecting the test you named from the editor Command Palette.
2. **Verify data was saved in S3**
    - Monitor the editor "OUTPUT" for a "Status: Succeeded" to ensure that there were no errors in your function code.
    - Navigate to your S3 bucket "magnificent7-market-data".
    - Look for path: raw/magnificent7/trading_date=YYYY-MM-DD/market_data.json
    - Open the `market_data.json` file and validate that the data matches the intented structure and is complete. Compare it to [data/market_data.json](data/market_data.json).

[market_data.json](data/market_data.json)
```json
{
  "trading_date": "2024-12-13",
  "data_collection_time": "2024-12-13T20:59:05.268375-05:00",
  "companies": {
    "AAPL": {
      "name": "Apple Inc.",
      "market_cap": 3750689160990.0,
      "shares_outstanding": 15115823000,
      "currency": "usd",
      "description": "Apple is among the largest companies in the world, with a broad portfolio of hardware and software products targeted at consumers and businesses. Apple's iPhone makes up a majority of the firm sales, and Apple's other products like Mac, iPad, and Watch are designed around the iPhone as the focal point of an expansive software ecosystem. Apple has progressively worked to add new applications, like streaming video, subscription bundles, and augmented reality. The firm designs its own software and semiconductors while working with subcontractors like Foxconn and TSMC to build its products and chips. Slightly less than half of Apple's sales come directly through its flagship stores, with a majority of sales coming indirectly through partnerships and distribution.",
      "trading_data": {
...
```

The function organizes collected data into a hierarchical JSON structure that facilitates downstream analysis. If the data is there, we can proceed to collect data for the last four years. 


### Trading Date Generator Lambda Function
The Trading Date Generator function creates lists of valid market trading dates, serving as a critical component for orchestrating historical data collection. This function can generate either a complete trading history for the past four years or a specific date range as needed.


##### Implementation Steps
1. **Create the Lambda Function**
   - Navigate to the AWS Lambda console and create a new function:
   ```yaml
   Function Name: trading-dates-generator
   Runtime: Python 3.13
   Architecture: x86_64
   Permissions: Create new role with basic Lambda permissions
   Memory: 128 MB (default)
   Timeout: 3 minutes
   ```
2. **Deploy Function Code**
    - Paste the following function code into the text editor and Deploy the changes.

        [trading-dates-generator/lambda_function.py](services/lambda/trading-dates-generator/lambda_function.py)
        ```python
        import json
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        def generate_trading_dates():
            """
            Generate list of trading dates from today back to 4 years ago.
            
            The function creates a list of dates excluding weekends, working
            backward from today. For 4 years of daily data (excluding weekends),
            we expect around 1,040 dates (260 trading days per year × 4 years).
            
            Returns:
                list: ISO format dates (YYYY-MM-DD) representing trading days
            """
            ...
        ```


### Orchestration with AWS Step Functions

AWS Step Functions provides the orchestration layer for our historical data collection pipeline, managing the execution flow between our Lambda functions. This serverless workflow engine ensures reliable data collection across extended time periods while providing robust error handling and execution visibility.

#### Core Architecture Components
A Step Functions state machine coordinates the workflow through two key Lambda functions:

1. Trading Dates Generator: Creates a list of valid market trading dates
2. Market Data Collector: Retrieves market data for each trading date

The state machine implements a Map state pattern, processing dates either sequentially or in parallel based on configuration. This design ensures both reliability and scalability while maintaining precise control over API rate limits.

#### Step Functions State Machine Creation
To create the state machine that orchestrates our data collection workflow, follow these detailed implementation steps.

1. Navigate to the AWS Step Functions Console
   - Select "State machines" from the left navigation
   - Click "Create state machine"
   - Choose "Write workflow in code" for the authoring method
   - Select "Standard" for the state machine type

2. Define the State Machine Logic
    Copy the following state machine defintion.

    [historical-data-collection/code.json](services/step-functions/historical-data-collection/code.json)
    ```json
    {
    "Comment": "Historical Data Collection Workflow",
    "StartAt": "GenerateDates",
    "States": {
      "GenerateDates": {
        "Type": "Task",
        "Resource": "arn:aws:lambda:us-west-2:371484867872:function:trading-dates-generator",
        "Next": "ProcessDates"
      },
      "ProcessDates": {
        "Type": "Map",
        "ItemsPath": "$.dates",
        "MaxConcurrency": 10,
    ...
    ```

#### IAM Role Configuration for Step Functions

The Step Functions state machine requires specific IAM permissions to invoke the Lambda functions in our workflow. We'll configure these permissions through the AWS IAM Console.

Navigate to the IAM console and locate the role that was automatically created when you set up the state machine. The role name typically follows the pattern `StepFunctions-HistoricalDataCollection-Role-[Random String]`.

First, verify the base role configuration:

The role should already include the basic Step Functions execution policy (`AWSStepFunctionsFullAccess`). We need to add specific Lambda invocation permissions for our functions.

Create a new inline policy for Lambda invocation permissions:

1. In the IAM console, select the Step Functions role
2. Navigate to the "Add permissions" dropdown and select "Create inline policy"
3. Choose the JSON editor and enter the following policy document:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:us-west-2:ACCOUNT-ID:function:trading-dates-generator",
                "arn:aws:lambda:us-west-2:ACCOUNT-ID:function:magnificent7-historical-data-collector"
            ]
        }
    ]
}
```

#### Executing the State Machine
After configuring your state machine, you can initiate the historical data collection process through the AWS Step Functions console. The execution will coordinate between the date generator and data collector functions to systematically gather market data.

Navigate to the AWS Step Functions console and select your "historical-data-collection" state machine. Click "Start execution" to begin the process. For a complete historical data collection covering the past four years, you can use an empty JSON object as the input:

```json
{}
```

#### Monitoring the Execution
The Step Functions console provides a visual representation of your workflow execution. The process unfolds in several stages:

First, the date generator Lambda function executes, producing a list of valid trading dates. You'll see this represented in the "GenerateTradingDates" state, typically completing within seconds.

Next, the "ProcessTradingDates" Map state begins processing the date list. With the MaxConcurrency parameter set to 10, the workflow processes ten dates simultaneously. The console displays a progress indicator showing the percentage of completed executions.
Each date processing task appears as a separate execution branch, with real-time status updates indicating success or failure. The visual interface makes it easy to identify any issues that require attention.

#### Performance Considerations
The complete data collection process for four years of historical data requires significant execution time. With each Lambda function taking approximately 9 minutes to process a single date, and considering the 15-minute Lambda timeout limit, the entire workflow typically requires about 15 hours to complete with MaxConcurrency set to 10.

#### Recovery Process
In the event that your state machine encounters errors or fails to complete, a separate recovery workflow is available. This alternative implementation uses [trading-dates-generator-range/lambda_function.py](services/lambda/trading-dates-generator-range/lambda_function.py). This allows you to target specific date ranges for data collection.

[trading-dates-generator-range/lambda_function.py](services/lambda/trading-dates-generator-range/lambda_function.py)
```python
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def generate_trading_dates():
    """
    Generate list of trading dates from a range.
    
    The function creates a list of dates excluding weekends from a range based on inputs.
    
    Returns:
        list: ISO format dates (YYYY-MM-DD) representing trading days
    """
    ...
```

To implement the recovery workflow:
1. Create a new lambda function and new state machine following the same steps outlined previously.
2. Use the modified state machine definition that references the trading-dates-generator-range Lambda function.

    [historical-data-collection-missing-data/code.json](services/step-functions/historical-data-collection-missing-data/code.json)
    ```json
    {
    "Comment": "Historical Data Collection Workflow - Missing Dates",
    "StartAt": "GenerateDates",
    "States": {
      "GenerateDates": {
        "Type": "Task",
        "Resource": "arn:aws:lambda:us-west-2:371484867872:function:trading-dates-generator-range",
        "Next": "ProcessDates"
      },
      "ProcessDates": {
        "Type": "Map",
        "ItemsPath": "$.dates",
        "MaxConcurrency": 10,
    ```
3. Provide the specific date range where data collection needs to be repeated

This recovery mechanism ensures you can efficiently address any gaps in your historical data collection without reprocessing the entire date range.

## Processing Pipeline

### AWS Glue Data Catalog Configuration

The processing pipeline leverages AWS Glue to transform our raw JSON market data into optimized formats for both analysis and storage. This approach creates two distinct data paths: one optimized for Athena queries through partitioned Parquet files, and another designed for efficient Redshift loading using unpartitioned Parquet files.

#### Data Transformation Strategy

AWS Glue serves as our primary transformation engine, handling the complexity of processing nested JSON structures into analytics-ready formats. The transformation process addresses several key requirements:

First, Glue flattens our nested JSON structures into normalized table formats, making the data more accessible for analysis. This process splits the complex JSON structure into discrete tables that align with our analytical needs while maintaining the relationships between different data elements.

Second, the transformation converts our data into the Parquet columnar format, which provides significant performance benefits for both storage and querying. Parquet's columnar structure is particularly well-suited for analytical workloads, allowing for efficient compression and faster query execution.

#### Dual Pipeline Architecture

Our processing pipeline implements a dual-path strategy to optimize for different analytical needs:

For Athena-based analytics, we create partitioned Parquet files organized by trading date. This structure enables Athena to leverage partition pruning, significantly reducing the amount of data scanned during queries and improving query performance while minimizing costs.

For Redshift integration, we generate unpartitioned Parquet files. This approach aligns with Redshift's native distribution mechanisms, allowing the data warehouse to manage data distribution according to its own optimization strategies.

The processing flow follows this sequence:

1. Raw JSON Processing
   - Source data is read from S3 in its original JSON format
   - The nested structure is parsed and validated
   - Data is transformed into normalized table structures

2. Athena-Optimized Path
   - Data is partitioned by trading date
   - Parquet files are generated with partition structure
   - The Glue Data Catalog is updated to reflect the partitioned schema

3. Redshift-Optimized Path
   - Data is transformed into unpartitioned Parquet files
   - Files are organized for efficient Redshift COPY operations
   - The structure aligns with Redshift's distribution strategy

#### Benefits of Implementation

This dual-pipeline approach delivers several significant advantages. The partitioned structure for Athena enables efficient query execution through partition pruning, while the unpartitioned format for Redshift allows the data warehouse to optimize data distribution according to its internal mechanisms.

The transformation to Parquet format provides enhanced compression, reducing storage costs and improving query performance. The simplified data structure also makes maintenance and troubleshooting more straightforward, as the data is organized in a clear, logical manner.

The use of AWS Glue as our transformation engine provides robust error handling and monitoring capabilities, ensuring data quality and completeness throughout the processing pipeline. The resulting data structures are both performant and maintainable, setting a strong foundation for our analytical workloads.

### Creating the Glue Data Catalog Database

The AWS Glue Data Catalog serves as a central metadata repository for our market data assets. Creating a dedicated database in the Data Catalog establishes an organized foundation for our data processing pipeline. The database will store table definitions and schemas that our ETL jobs will populate.

#### Database Creation Process

1. Navigate to the AWS Glue console and locate the Data Catalog section in the left navigation panel. 
2. Select "Databases" to access the database management interface. 
3. From here, create a new database by selecting "Add database."
4. Enter "market_data" as the name.

At this stage, we will not create any tables within the database, as our ETL job will generate these automatically based on our data structure.

The database will eventually contain table definitions for:
- Concentration metrics tracking market cap distributions
- Company details storing fundamental information
- Daily trading data capturing price movements
- Magnificent 7 specific metrics
- Data collection status information

Our ETL job will create these table definitions dynamically during its first run, ensuring that the schemas precisely match our transformed data structure. This approach maintains consistency between our data and metadata while reducing manual configuration requirements.

### Glue ETL Job Development

The ETL job transforms our raw JSON market data into optimized Parquet files while maintaining data integrity and relationships. We'll use Glue's visual interface to configure the transformation pipeline, implementing SQL-based transformations for each target table. Our data processing strategy requires two separate ETL jobs to optimize data for different analytical use cases. We'll create a processing pipeline for Athena queries and an analytics pipeline for Redshift integration.


#### Creating the ETL Jobs

Navigate to the AWS Glue console and select "ETL jobs" from the left navigation panel. Create a new job by clicking "Create job" and selecting "Visual with a source and target" as the job type. Name the job optimized for Athena queries "processing-pipeline" to reflect its purpose. Name the job optimized for Redshift loading "analytics-pipeline" to reflect its purpose.

#### Source Configuration

Begin by configuring the data source for our ETL pipeline. Select Amazon S3 as the source type and specify the following parameters:

Source Configuration:
- Data Format: JSON
- S3 URL Path: s3://magnificent7-market-data/raw/magnificent7/
- Enable Schema Inference: Yes

After configuring the source, click "Infer schema" to allow Glue to analyze our JSON structure. This automatic schema inference provides the foundation for our transformations.

#### Schema Modification

Add a Change Schema node to review and modify the inferred schema. This step is crucial for ensuring proper data type handling in our transformations. Within the schema editor:

1. Locate the Companies array in the schema
2. For each company entry, modify the following fields:
   - market_cap: Change to DOUBLE
   - shares_outstanding: Change to DOUBLE

These modifications ensure accurate numerical processing in our subsequent SQL transformations.

#### Transformation Implementation

Create separate transformation branches for each target table. Each branch should originate from the Change Schema node, implementing specific SQL transformations for its target table.

Create SQL transformation nodes for:
1. Concentration Metrics: Market-wide concentration analysis

[concentration_metrics.sql](services/glue/concentration_metrics.sql)
```sql
SELECT 
    -- Get trading_date from root level for consistency
    trading_date,
    -- Extract fields from concentration_metrics structure
    concentration_metrics.total_mag7_market_cap,
    concentration_metrics.sp500_total_market_cap,
    concentration_metrics.mag7_pct_of_sp500,
    concentration_metrics.mag7_companies_count
FROM myDataSource
WHERE concentration_metrics.total_mag7_market_cap IS NOT NULL
```

2. Company Details: Fundamental company information

[company_details.sql](services/glue/company_details.sql)
```sql
SELECT 
    trading_date,
    stack(7,
        'AAPL', companies.AAPL.name, 
        CAST(companies.AAPL.market_cap AS DOUBLE),
        CAST(companies.AAPL.shares_outstanding AS DOUBLE),
        companies.AAPL.currency, 
        companies.AAPL.description,

        'MSFT', companies.MSFT.name, 
        CAST(companies.MSFT.market_cap AS DOUBLE),
        CAST(companies.MSFT.shares_outstanding AS DOUBLE),
        companies.MSFT.currency, 
        companies.MSFT.description,
        ...
```

3. Daily Trading: Price and volume data

[daily_trading.sql](services/glue/daily_trading.sql)
```sql
SELECT 
    -- Get trading date from the nested trading_data to match the actual trading day
    companies.AAPL.trading_data.date AS trading_date,
    -- Use stack to transform company trading data into rows
    stack(7,
        'AAPL', companies.AAPL.trading_data.open_price, companies.AAPL.trading_data.high_price, companies.AAPL.trading_data.low_price, companies.AAPL.trading_data.close_price, companies.AAPL.trading_data.volume, companies.AAPL.trading_data.vwap,
        'MSFT', companies.MSFT.trading_data.open_price, companies.MSFT.trading_data.high_price, companies.MSFT.trading_data.low_price, companies.MSFT.trading_data.close_price, companies.MSFT.trading_data.volume, companies.MSFT.trading_data.vwap,
        ...
```

4. Magnificent 7 Metrics: Specific metrics for our focus companies

[magnificent7_metrics.sql](services/glue/magnificent7_metrics.sql)
```sql
SELECT
    trading_date,
    -- Extract from the rankings.by_market_cap array which is already ordered
    stack(7,
        rankings.by_market_cap[0].ticker, rankings.by_market_cap[0].market_cap, rankings.by_market_cap[0].pct_of_mag7, rankings.by_market_cap[0].pct_of_sp500, 1,
        rankings.by_market_cap[1].ticker, rankings.by_market_cap[1].market_cap, rankings.by_market_cap[1].pct_of_mag7, rankings.by_market_cap[1].pct_of_sp500, 2,
        ...
```

5. Failed Collections: Data quality tracking

[failed_connections.sql](services/glue/failed_connections.sql)
```sql
SELECT
    trading_date,
    CAST(NULL as STRING) as ticker,
    CASE 
        WHEN sp500_details.failed_count = 0 AND concentration_metrics.total_mag7_market_cap IS NULL 
        THEN 'Market Holiday'
        WHEN sp500_details.failed_count = 0 
        THEN 'No collection failures'
        ELSE 'Failed collections present'
    END as reason
FROM myDataSource
```

#### Target Configuration

For each transformation branch, configure an S3 target node with the following settings:

Base Configuration:
- Format: Parquet
- Compression: Snappy

For Processed Data (Athena-Optimized):
- S3 Target Location: s3://magnificent7-market-data/processed/[table_name]/
- Database: market_data
- Table name: [table_name]
- Partition keys: trading_date
- Data Catalog update options: "Create a table in the Data Catalog and on subsequent runs, keep existing schema and add new partitions"

For Analytics Data (Redshift-Optimized):
- S3 Target Location: s3://magnificent7-market-data/analytics/[table_name]/
- Data Catalog update options: "Do not update Data Catalag"
- Partition keys: Delete key

#### Job Optimization Settings

Configure the following job settings to optimize performance and resource utilization:

Performance Settings:
- Worker Type: Standard
- Number of Workers: Auto-scaling (2-10)
- Job Bookmark: Enabled
- Job Metrics: Enabled

The job bookmark feature ensures efficient processing of new data while avoiding reprocessing of previously transformed data. This is particularly important for our incremental data loading strategy.


### IAM Role Configuration for Glue ETL Job

The Glue ETL job requires specific permissions to access S3 resources and manage the Data Catalog. We'll configure these permissions through two policies attached to a dedicated IAM role.

#### Creating the IAM Role

First, navigate to the IAM console to create a new role for the Glue ETL job:

1. Select "Create role" in the IAM console
2. Choose "AWS service" as the trusted entity type
3. Select "Glue" from the use case options
4. Name the role "GlueETL-MarketData-Role"

#### Attaching the AWS Managed Policy

Attach the AWS managed policy that provides basic Glue service permissions:

1. Search for and select "AWSGlueServiceRole" from the policy list
2. This policy grants fundamental permissions required for Glue service operations

#### Creating the Custom Policy

Next, create a custom policy to grant specific permissions for S3 access and Data Catalog management:

1. Select "Create inline policy"
2. Choose the JSON editor and enter the following policy document:

```json
{
   "Version": "2012-10-17",
   "Statement": [
       {
           "Effect": "Allow",
           "Action": [
               "s3:GetObject",
               "s3:ListBucket",
               "s3:PutObject",
               "s3:DeleteObject"
           ],
           "Resource": [
               "arn:aws:s3:::magnificent7-market-data",
               "arn:aws:s3:::magnificent7-market-data/*"
           ]
       },
       {
           "Effect": "Allow",
           "Action": [
               "glue:GetTable",
               "glue:CreateTable",
               "glue:UpdateTable",
               "glue:DeleteTable",
               "glue:BatchCreatePartition",
               "glue:CreatePartition",
               "glue:BatchDeletePartition"
           ],
           "Resource": [
               "arn:aws:glue:*:ACCOUNT-ID:catalog",
               "arn:aws:glue:*:ACCOUNT-ID:database/*",
               "arn:aws:glue:*:ACCOUNT-ID:table/*"
           ]
       }
   ]
}
```
Name this policy "GlueETL-MarketData-CustomPolicy" and provide a description explaining its purpose.

#### Attaching the Role to the Glue Job
Return to the Glue ETL job configuration and assign the newly created role:

1. Navigate to the job's "Properties" tab
2. Under "Security configuration, script libraries, and job parameters"
3. Select the "GlueETL-MarketData-Role" from the IAM Role dropdown

You can now save the Glue job.

#### Validation and Testing

Before deploying the job:
1. Validate all node connections
2. Review transformation logic
3. Verify partition configurations
4. Test with a sample dataset

This configuration creates a robust ETL pipeline that maintains data quality while optimizing for both Athena queries and Redshift loading. The dual output strategy ensures optimal performance for both analytical paths while maintaining consistent data structures throughout our pipeline.


#### Verifying Analytics Pipeline Output
After running both ETL jobs, we need to verify that our data transformation was successful and the output files meet our requirements. Let's examine both the processed and analytics data structures to ensure they're optimized for their respective use cases.

The analytics pipeline should produce unpartitioned Parquet files in a flat structure, optimized for Redshift loading. Navigate to your S3 bucket's analytics directory and verify:

Navigate to s3://magnificent7-market-data/analytics/[table_name]/ and examine each table's directory. The files should follow these specifications:

- All Parquet files should be at the root level of each table directory
- No nested folder structures or date-based partitioning
- Files should use consistent naming patterns
- Each table directory should contain a complete set of data

The file sizes should reflect reasonable compression ratios for Parquet format with Snappy compression. For our market data, typical file sizes should range from several hundred kilobytes to a few megabytes, depending on the data volume for each trading date.

#### Examining Processed Pipeline Output

The processed pipeline creates partitioned data structures optimized for Athena queries. Examine s3://magnificent7-market-data/processed/ to verify:

Each table's directory should show a clear partition structure organized by trading date. The hierarchy should follow this pattern:

s3://magnificent7-market-data/processed/[table_name]/trading_date=[YYYY-MM-DD]/[data files]

This structure enables Athena to leverage partition pruning for efficient query execution. Each partition should contain properly formatted Parquet files with Snappy compression.

#### Data Quality Verification

With our file structures confirmed, we can proceed to validate the data quality using Athena. This will allow us to:

- Confirm the data types are correctly mapped
- Verify the completeness of our dataset across all trading dates
- Ensure our transformations maintained data accuracy
- Validate the relationships between our different tables

The next section will cover the specific Athena queries we'll use to perform these validation checks, ensuring our data is ready for both ad-hoc analysis and data warehouse integration.

## Analytics Worklflow

### Amazon Athena Data Validation and Analysis

After populating our Data Catalog through the Glue ETL process, we can use Amazon Athena to validate our data transformation and begin exploring market concentration patterns. Let's examine our dataset through a series of analytical queries that verify data quality and reveal market insights.

First, navigate to the Amazon Athena console and select the `market_data` database from the catalog. The schema browser should display our five core tables: concentration_metrics, company_details, daily_trading, magnificent7_metrics, and failed_collections.

Let's begin with fundamental validation queries to ensure our data transformation succeeded and maintained data integrity:

#### Data Completeness Verification

These queries examine our data coverage, confirming we have successfully collected and transformed data across our intended time range:

[earliest_latest_date.sql](services/athena/earliest_latest_date.sql)
```sql
-- Check date coverage and completeness
SELECT 
    MIN(trading_date) as earliest_date,
    MAX(trading_date) as latest_date,
    COUNT(DISTINCT trading_date) as trading_days
FROM market_data.concentration_metrics;
```

[all_magnificent7.sql](services/athena/all_magnificent7.sql)
```sql
-- Verify we have data for all Magnificent 7 companies each trading day
SELECT 
    trading_date,
    COUNT(DISTINCT ticker) as company_count
FROM market_data.company_details
GROUP BY trading_date
HAVING COUNT(DISTINCT ticker) != 7
ORDER BY trading_date;
```

[market_cap_extremes.sql](services/athena/market_cap_extremes.sql)
```sql
-- Check for any extreme values that might indicate transformation issues
SELECT 
    trading_date,
    ticker,
    market_cap
FROM market_data.company_details
WHERE market_cap > 5e12  -- More than $5 trillion
   OR market_cap < 1e11; -- Less than $100 billion
```

[min_max_concentration.sql](services/athena/min_max_concentration.sql)
```sql
-- Check for minimum and maximum bounds of concenrations data
SELECT 
    MAX(mag7_pct_of_sp500) AS highest_value,
    MIN(mag7_pct_of_sp500) AS lowest_value
FROM market_data.concentration_metrics;
```

These validation queries serve multiple purposes. They confirm the accuracy of our data transformation process, identify any potential quality issues requiring attention, and provide initial insights into our market concentration analysis. The queries leverage Athena's ability to process partitioned data efficiently, allowing us to analyze large datasets while minimizing costs through partition pruning.

After confirming data quality, we can proceed with more detailed market analysis queries to examine trends in market concentration and individual company performance. These analytical queries will form the foundation for our visualization work in QuickSight and our more complex analyses in Redshift.


#### Redshift Serverless Setup
With serverless, you only pay for the compute time you actually use when querying your data, and AWS automatically handles all the scaling. This is particularly well-suited for your use case because you'll likely have periodic data loads (daily after market close) and occasional analytical queries, rather than constant, predictable workloads that would justify a provisioned cluster. The advantage of using Parquet files is that they're optimized for analytics and more efficient to load than our original JSON files.

Let me walk you through creating a Redshift serverless workspace step by step. A workspace in Redshift serverless is where your data and compute resources live


First, navigate to Redshift in the AWS Console:

Open the AWS Management Console
Search for "Redshift" in the search bar
Click on "Amazon Redshift"


Start the serverless setup:

Look for "Serverless dashboard" in the left navigation
Click "Create serverless workspace"


Configure basic workspace settings:

Name your workspace (e.g., "market-analysis")
In the namespace name field, enter a similar name (namespaces help organize resources)
Leave "Turn on Enhanced VPC routing" unchecked for now
For the security and encryption section, choose "AWS owned key" for simplicity


For network and security:

Either choose an existing VPC or let AWS create a new one
For subnet selection, choose two or more subnets in different availability zones for reliability
Under "VPC security group", either select an existing one or create new
For database authentication, choose "AWS Identity and Access Management (IAM)"


Configure your limits and cost controls:

Base capacity: Start with 8 RPUs (Redshift Processing Units)
Maximum RPU capacity: Set to 64 (you can adjust this later based on usage)
For cost control, enable "Turn on cost control"
Set a daily cost limit (e.g., start with $50 to be safe)


Review and create:

Review all settings
Click "Create workspace"

After creation (which takes about 5-10 minutes), we'll need to:

Set up permissions for Zero-ETL integration with S3
Design your table schema for the market data
Configure the data transfer from S3

#### Setup Redshift permissions
Configure IAM roles and policies to allow Redshift to read from your S3 bucket


#### Schema and Table Creation
Let's design a schema adn tables that effectively matches the schema we defined in Glue.
We're using a dedicated schema market_data to organize our tables logically

First, let's access the query editor:

In the Redshift console, look at the left navigation pane
Click on "Query editor v2" (this is Redshift's modern SQL workspace)
Select your serverless workspace that we created earlier
You may need to authenticate - choose "Federated user login" if using your AWS console credentials

Once you're in the query editor, you'll see a blank workspace where we can write and execute our SQL. Let's break down the schema creation into logical steps and execute them one at a time so we can verify each step succeeds:

* Create Schema
```sql
-- First create a dedicated schema for our market data
CREATE SCHEMA market_data;
```
Click "Run" or press Ctrl+Enter (Cmd+Enter on Mac) to execute this command. You should see a success message.

Now we can create each table one by one. This methodical approach helps us catch any potential issues early. Let's start with the trading_dates table:

* Create tables matching the Glue schema and folder structure. 
    - We'll use DISTKEY and SORTKEY on trading_date since this is our main temporal dimension and most queries will likely filter or join on this column.
    - Open a Notebook and write it Create table command in it's own cell.
```sql
-- Company details table for point-in-time snapshots
CREATE TABLE market_data.company_details (
    trading_date DATE,
    ticker VARCHAR(10),
    company_name VARCHAR(255),
    market_cap NUMERIC(20,2),
    shares_outstanding NUMERIC(20,2),
    currency VARCHAR(3),
    description TEXT,
    PRIMARY KEY (trading_date, ticker)
)
DISTKEY(trading_date)
COMPOUND SORTKEY(trading_date, ticker);

-- Daily trading data for each company
CREATE TABLE market_data.daily_trading (
    trading_date DATE,
    ticker VARCHAR(10),
    open_price NUMERIC(20,2),
    high_price NUMERIC(20,2),
    low_price NUMERIC(20,2),
    close_price NUMERIC(20,2),
    volume BIGINT,
    vwap NUMERIC(20,2),
)
DISTKEY(trading_date)
COMPOUND SORTKEY(trading_date, ticker);


-- Magnificent 7 specific metrics
CREATE TABLE market_data.magnificent7_metrics (
    trading_date DATE,
    ticker VARCHAR(10),
    market_cap NUMERIC(20,2),
    pct_of_mag7 NUMERIC(5,2),
    pct_of_sp500 NUMERIC(5,2),
    ranking INTEGER,
)
DISTKEY(trading_date)
COMPOUND SORTKEY(trading_date, ticker);

-- Daily market concentration metrics
CREATE TABLE market_data.concentration_metrics (
    trading_date DATE,
    total_mag7_market_cap NUMERIC(20,2),
    sp500_total_market_cap NUMERIC(20,2),
    mag7_pct_of_sp500 NUMERIC(5,2),
    mag7_companies_count INTEGER
)
DISTKEY(trading_date)
SORTKEY(trading_date);

-- Failed collections tracking for monitoring
CREATE TABLE market_data.failed_collections (
    trading_date DATE,
    ticker VARCHAR(10),
    reason TEXT,
)
DISTKEY(trading_date)
SORTKEY(trading_date);
```

After each successful table creation, you'll see a confirmation message. You can verify the table was created by looking at the schema browser on the left side of the query editor - expand the market_data schema and you should see your new table.

Each table has a clear purpose:

trading_dates: Tracks all collection dates and overall statistics
company_details: Point-in-time company information
daily_trading: Daily price and volume data
magnificent7_metrics: Specific metrics for the Mag 7 companies
concentration_metrics: Daily market concentration analysis
failed_collections: Error tracking for data quality monitoring




#### S3 Event Integration
* S3 event integration setup
Open the Amazon Redshift console and navigate to "S3 event integrations" in the left navigation pane
Click "Create Amazon S3 event integration" and enter these recommended values:
For the Integration Details:

Integration name: market-data-s3-integration
(This name clearly indicates both the data type and integration type)
Description: "Automatic ingestion of daily market data from S3 to Redshift for Magnificent 7 and S&P 500 analysis"
(This description helps future users understand the purpose)

For the Source and Target:

Source S3 bucket: Select your bucket containing the market data
(This should be the bucket where your Lambda is writing the daily JSON files)
For the S3 prefix, enter: analytics/
(This matches your Lambda's output path structure)
Amazon Redshift data warehouse: Select your serverless workspace we just created
(If the "Fix it for me" option appears for permissions, select it)

Review the configuration carefully. The integration will watch for new files within the folder:
analytics/

* Auto-copy job configuration
Let me walk you through setting up COPY jobs for each of our tables. We'll need to create multiple COPY jobs since our Parquet data needs to be parsed differently for each table. Let's start with the most straightforward table and work our way up.

We we will setup our COPY jobs in the query editor:

Go to the Redshift console
Click "Query editor v2" in the left navigation
Select your serverless workspace
You'll see a blank query editor where we can write our COPY commands

```sql
COPY market_data.concentration_metrics
FROM 's3://magnificent7-market-data/analytics/concentration_metrics/'
IAM_ROLE default
FORMAT PARQUET
JOB CREATE concentration_metrics_load
AUTO ON;

COPY market_data.company_details
FROM 's3://magnificent7-market-data/analytics/company_details/'
IAM_ROLE default
FORMAT PARQUET
JOB CREATE company_details_load
AUTO ON;
```

let's verify these first two COPY jobs are working correctly. You can check their status with:
```sql
SELECT * FROM sys_copy_job 
WHERE job_name IN ('concentration_metrics_load', 'company_details_load')
ORDER BY job_name;
```

If this is functioning properly we can run the other COPY commands.
```sql
COPY market_data.magnificent7_metrics
FROM 's3://magnificent7-market-data/analytics/magnificent7_metrics/'
IAM_ROLE default
FORMAT PARQUET
JOB CREATE magnificent7_metrics_load
AUTO ON;

COPY market_data.failed_collections
FROM 's3://magnificent7-market-data/analytics/failed_collections/'
IAM_ROLE default
FORMAT PARQUET
JOB CREATE failed_collections_load
AUTO ON;

COPY market_data.daily_trading
FROM 's3://magnificent7-market-data/analytics/daily_trading/'
IAM_ROLE default
FORMAT PARQUET
JOB CREATE daily_trading_load
AUTO ON;
```

After creating these jobs, we can monitor their status using this query:
```sql
SELECT *
FROM sys_copy_job
ORDER BY job_name;
```

We can also check row counts in the editor tree view to ensure everything loaded properly
If we encounter any issues, we can easily rerun a cell in our notebook.

So far we have used the query editor to configure our data warehouse. With our tree view updating successfully witht he proper data from S3 let's analyze our data using the Query editor.


#### Analysis in Redshift Query Editor v2
* Data exploration queries

After each load, we can run a simple validation query like:
```sql
SELECT 
    MIN(trading_date) as earliest_date,
    MAX(trading_date) as latest_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT trading_date) as unique_dates
FROM market_data.[table_name];
```

```sql
-- Validate our market concentration calculations
SELECT 
    c.trading_date,
    ABS(c.mag7_pct_of_sp500 - 
        (SUM(m.market_cap) / c.sp500_total_market_cap * 100)) as pct_difference
FROM market_data.concentration_metrics c
JOIN market_data.magnificent7_metrics m 
    ON c.trading_date = m.trading_date
GROUP BY c.trading_date, c.mag7_pct_of_sp500, c.sp500_total_market_cap
HAVING pct_difference > .01;  -- Check for differences greater than 0.01%
```

* Time series analysis
* Market concentration calculations
* Query performance optimization
* Visualization capabilities within Query Editor
* Saving and sharing queries



### Data Visualization Solutions
how to visualize your market data in QuickSight using the AWS Glue Data Catalog. We'll be using the partitioned data structure in your processed/ directory since it's optimized for analytics. First, we need to set up QuickSight to work with our data. In QuickSight, we'll create what's called a "data set" that connects to our Glue Data Catalog tables. Think of this like building a bridge between where your data lives and where you want to analyze it.

#### Amazon QuickSight Implementation
* Data source configuration
Let's walk through the process:

In QuickSight, navigate to "Datasets" and click "New dataset"
From the data source options, select "Athena"

This is important because Athena is the service that lets QuickSight query data directly from your S3 buckets using the Glue Data Catalog schema


Create a new Athena data source:

Give it a name like "magnificent7-analytics"
Select the workgroup you want to use
Choose the catalog database "market_data" we created earlier


Once connected, you'll see your tables available:

concentration_metrics
company_details
daily_trading
magnificent7_metrics
failed_collections


First, let's create datasets for each table:

For concentration_metrics:

Go to Datasets > New dataset
Choose Athena as the source
Select your market_data database
Choose the concentration_metrics table
Click "Use in analysis" (not Edit/Preview)
Name it "Concentration Metrics"
This table will serve as a foundation for tracking overall market trends


For company_details:

Create another new dataset following the same initial steps
Select company_details table
Name it "Company Details"
This dataset contains the fundamental company information that helps contextualize market movements


For daily_trading:

Create another new dataset
Select daily_trading table
Name it "Daily Trading"
This contains the detailed price action data that we'll use for performance analysis


For magnificent7_metrics:

Create another new dataset
Select magnificent7_metrics table
Name it "Magnificent 7 Metrics"
This dataset specifically tracks the relative performance and rankings of our key companies


For failed_collections:

Create the final dataset
Select failed_collections table
Name it "Failed Collections"
This will help us track data quality and completeness


This setup creates a direct line from your S3 data through Athena to QuickSight, allowing you to create visualizations like:

Line charts showing the evolution of market concentration over time
Bar charts comparing different companies' market caps
Area charts displaying the Magnificent 7's percentage of the S&P 500
Trend analyses of trading volumes and price movements

* Field customization and calculations
For each dataset, follow these steps to convert the trading_date from a string to a proper date field:

Go to "Datasets" in QuickSight
Find one of your datasets (let's start with "Concentration Metrics")
Click the dataset name to open its settings
Click "Edit dataset"
In the data preparation interface, find the trading_date column
Click on the column header to see its properties
Under "Data type", change it from "String" to "Date"
QuickSight will ask you to specify the date format:

Your dates are in "YYYY-MM-DD" format
Select this format from the dropdown or specify it as a custom format


Click "Update" to apply the change
Click "Save & publish" to update the dataset

You'll need to repeat this process for each dataset since they all contain the trading_date field. This is important because properly formatted date fields enable you to:

Create time-based visualizations like trend lines
Use date-based filters (like "Last 30 days" or "Year to date")
Perform time intelligence calculations (like year-over-year comparisons)
Group data by different time periods (months, quarters, years)

After updating all datasets, any new visualizations you create will automatically recognize trading_date as a date field, giving you access to QuickSight's full range of time-based analysis features. 


* Dataset relationships - optional
et me explain how to work with multiple tables in QuickSight. When we want to analyze data across multiple tables, we need to create what's called a "joined dataset." Think of this like connecting different pieces of a puzzle to see the complete picture.
Here's how to create a dataset with multiple tables:

Start as before: go to "Datasets" and click "New dataset"
Select "Athena" as your data source and connect to your "market_data" database
When you select your first table (like concentration_metrics), click "Edit/Preview data" instead of directly using the data
In the data preparation interface, look for the "Add data" button (usually in the top left)

This opens the join interface where you can add more tables
Select another table (like company_details)
QuickSight will prompt you to define how these tables relate to each other


When joining tables, you'll need to specify the join conditions:

For example, concentration_metrics joins to company_details on trading_date
You can choose the type of join (inner, left, right, full) depending on your analysis needs
You can continue adding more tables and defining their relationships

This joined dataset becomes a single source for your analysis, allowing you to create visualizations that combine data from all your tables. For example, you could create:

A dashboard showing both market concentration and individual company performance
Analysis combining price data with market cap percentages
Comprehensive views of how companies move in and out of top rankings

* Tradeoffs between using joined tables versus separate tables
When you create separate datasets in QuickSight, you maintain more flexibility and better performance. Think of it like having a well-organized toolbox where each tool has its specific place. You can use each table independently when needed, and QuickSight allows you to create relationships between these separate datasets when building visualizations and dashboards.
Here's how it works with separate datasets:

Each dataset loads independently, which means faster data refreshes
You can create analysis-specific joins within visualizations using dataset relationships
QuickSight recognizes common fields (like trading_date and ticker) across datasets
You can choose different join types for different visualizations, rather than being locked into one join pattern

However, if you create one large joined dataset:

The entire dataset must load every time, even if you only need part of it
You're locked into specific join relationships that you defined upfront
Changes to join logic require modifying the entire dataset
Large joined datasets can become slower to query and more complex to maintain

For your market data analysis, separate datasets would work well because:

Your tables already have clear relationships through trading_date and ticker
Different analyses might need different combinations of the data
Some visualizations might only need data from one table
You maintain the flexibility to optimize each visualization's performance

When building dashboards, QuickSight lets you create "dataset relationships" that work similarly to database joins but are more flexible. You define these relationships in your analysis, not in the dataset itself, which gives you the power to:

Use different join types for different visualizations
Combine data only when needed
Maintain better query performance
Change relationships without rebuilding your datasets

#### QuickSight Visualizations
Let me explain what kinds of visualizations would be most meaningful for each dataset, based on the unique information each one contains and the insights we can draw from them.
For Concentration Metrics:

Line chart showing total S&P 500 market cap over time

This helps visualize the overall market growth or contraction
Adding the Magnificent 7 total market cap as a second line shows their growing influence


Area chart displaying the Magnificent 7's percentage of S&P 500

The filled area dramatically illustrates market concentration
Adding a reference line at key thresholds (like 30%) helps highlight significant milestones


KPI indicators showing current values compared to historical averages

Total market cap
Concentration percentage
Number of companies tracked



For Company Details:

Treemap of current market caps

Box size represents market cap value
Color intensity could show percentage change
Gives an immediate visual sense of relative company sizes


Historical line chart of shares outstanding

Shows how companies' capital structures change over time
Can reveal stock splits, buybacks, or new issuance


Bar chart comparing current market caps

With a secondary axis showing shares outstanding
Helps understand how share count relates to total value



For Daily Trading:

Candlestick chart showing price movement

Includes open, high, low, close prices
Add volume bars at the bottom
Consider adding moving averages


Line chart comparing volume trends across companies

Helps identify which stocks are most actively traded
Can spot unusual trading activity


Heat map of daily returns

Columns for companies
Rows for trading days
Colors indicating positive/negative returns



For Magnificent 7 Metrics:

Stacked bar chart showing percentage composition

Each company's contribution to total Magnificent 7 market cap
Track how relative weights change over time


Bump chart tracking ranking changes

Shows how companies move up and down in relative position
Particularly interesting around major market events


Line chart comparing individual percentages of S&P 500

Individual lines for each company's market cap percentage
Helps identify which companies drive concentration most



For Failed Collections:

Calendar heat map

Shows days with collection issues
Color intensity based on number of failures


Bar chart of failure reasons

Categorize and count different types of failures
Helps identify systematic issues


Success rate trend line

Track data collection quality over time
Spot periods of reliability issues

#### Visualizatoin Best Practices
For all these visualizations, I recommend adding:

Clear titles that explain what we're looking at
Appropriate date filters to focus on relevant time periods
Tooltips with detailed information on hover
Reference lines or bands for important thresholds
Consistent color schemes across related visualizations


## Future Enhancements
* Add more data sources to data warehouse
* Access propriety S&P 500 constituants list
* Dynamic S&P 500 constituants based on trading day
* Only pull trading data on non-market holidays
* Add GOOG aggregate ticker data to compare against GOOGL
    * Current market cap data represents all of Alphabet
    * Price divergance is usually minimal (1-2%)
* Track ticker changes for non-Magnificent 7 companies
* Real-time data pipeline
* Automate daily market close data collection using EventBridge
* Advanced analytics, dashboards, and data stories
* Additional performance optimizations