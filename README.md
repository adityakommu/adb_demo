# Search Keyword Performance Analyzer

A Python application that analyzes Adobe Analytics hit-level data to calculate revenue from external search engines (Google, Yahoo, Bing/MSN) and identifies the best performing keywords based on revenue.

## Business Problem

**Question**: How much revenue is the client getting from external Search Engines, such as Google, Yahoo and MSN, and which keywords are performing the best based on revenue?

## Solution Overview

This application:
1. Reads tab-separated hit-level data files
2. Tracks user sessions by IP address
3. Identifies traffic from external search engines (Google, Yahoo, Bing/MSN)
4. Extracts search keywords from referrer URLs
5. Attributes revenue to keywords when purchases occur (event_list contains "1")
6. Outputs results sorted by revenue (descending)

## Project Structure

```
├── search_keyword_performance.py   # Main application
├── test_search_keyword_performance.py  # Unit tests
├── lambda_handler.py               # AWS Lambda handler
├── template.yaml                   # AWS SAM deployment template
├── requirements.txt                # Python dependencies
├── data.sql                        # Sample data file
└── README.md                       # This file
```

## Requirements

- Python 3.8+
- No external dependencies for local execution (uses standard library only)
- boto3 (for AWS Lambda deployment)

## Local Usage

### Basic Usage

```bash
python search_keyword_performance.py data.sql
```

### With Custom Output File

```bash
python search_keyword_performance.py data.sql -o custom_output.tab
```

### Verbose Mode

```bash
python search_keyword_performance.py data.sql -v
```

## Output Format

The application produces a tab-delimited file with:

| Column | Description | Example |
|--------|-------------|---------|
| Search Engine Domain | The search engine domain | google.com |
| Search Keyword | The search term used | ipod |
| Revenue | Total revenue attributed | 290.00 |

Output filename format: `YYYY-mm-dd_SearchKeywordPerformance.tab`

## Running Tests

```bash
python -m pytest test_search_keyword_performance.py -v
```

Or using unittest:

```bash
python -m unittest test_search_keyword_performance -v
```

## AWS Deployment

### Prerequisites

- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed
- An S3 bucket name for the deployment

### Deploy with SAM

```bash
# Build the application
sam build

# Deploy (first time - guided)
sam deploy --guided

# Deploy (subsequent deployments)
sam deploy
```

### Using the Lambda Function

**Option 1: S3 Trigger**
Upload a `.tsv` file to `s3://<bucket>/input/` and the Lambda will automatically process it.

**Option 2: Direct Invocation**
```bash
aws lambda invoke \
  --function-name search-keyword-processor-dev \
  --payload '{"input_bucket": "my-bucket", "input_key": "data.tsv"}' \
  response.json
```

**Option 3: API Gateway**
```bash
curl -X POST https://<api-id>.execute-api.<region>.amazonaws.com/Prod/process \
  -H "Content-Type: application/json" \
  -d '{"input_bucket": "my-bucket", "input_key": "data.tsv"}'
```

## Technical Approach

### Session Tracking
The application tracks user sessions by IP address. When a user arrives from a search engine, their search referral (domain + keyword) is stored. When they later make a purchase, the revenue is attributed to that original search keyword.

### Revenue Attribution Logic
1. Parse each row's referrer URL
2. If referrer is from Google/Yahoo/Bing, extract the search keyword
3. Store the search referral for that user (by IP)
4. When a purchase event (event_list contains "1") occurs, extract revenue from product_list
5. Attribute revenue to the user's original search keyword

### Search Engine Detection
Supported search engines and their query parameters:
- **Google**: `google.com` - parameters: `q`, `query`
- **Yahoo**: `search.yahoo.com` - parameters: `p`, `q`, `query`
- **Bing**: `bing.com` - parameters: `q`, `query`
- **MSN**: `msn.com` - parameters: `q`, `query`

## Scalability Considerations

### Current Limitations
The current implementation loads user sessions into memory, which works well for files up to a few GB but may face challenges with 10GB+ files.

### Recommended Improvements for Large Files

1. **Streaming Processing with Chunking**
   - Process file in chunks using pandas with `chunksize` parameter
   - Maintain session state in a lightweight database (SQLite/Redis)

2. **AWS EMR/Spark**
   - For files >10GB, migrate to PySpark on EMR
   - Enables distributed processing across multiple nodes
   - Example: Use Spark DataFrame with window functions for session tracking

3. **AWS Glue**
   - Serverless ETL service for large-scale data processing
   - Native Spark support with automatic scaling
   - Integration with S3 and data catalog

4. **Database-backed Sessions**
   - Replace in-memory dict with DynamoDB or Redis
   - Enables horizontal scaling of Lambda functions
   - Example architecture:
     ```
     S3 → Lambda (chunk processor) → DynamoDB (sessions) → Lambda (aggregator) → S3
     ```

5. **Pre-sorting Input Data**
   - If data is sorted by IP/session, process sequentially
   - Reduces memory requirements significantly

### Memory Optimization Tips
```python
# Use generators instead of lists
for chunk in pd.read_csv(file, chunksize=100000):
    process_chunk(chunk)

# Use more efficient data structures
from array import array
revenue_values = array('d')  # Instead of list of floats
```

## Sample Results

Based on the provided sample data:

```
Search Engine Domain    Search Keyword    Revenue
www.google.com          ipod              290.00
www.bing.com            zune              250.00
search.yahoo.com        cd player         190.00
```

**Total Search Engine Revenue: $730.00**

## Author


