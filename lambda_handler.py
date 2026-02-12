"""
AWS Lambda - Search Keyword Performance (Chunked Processing)
"""

import json
import boto3
import os
import tempfile
import pandas as pd
from datetime import datetime
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')

SEARCH_ENGINES = ['google.com', 'www.google.com', 'bing.com', 'www.bing.com',
                  'search.yahoo.com', 'yahoo.com', 'www.yahoo.com', 'msn.com']
USECOLS = ['ip', 'referrer', 'event_list', 'product_list']
DTYPES = {'ip': 'string', 'referrer': 'string', 'event_list': 'string', 'product_list': 'string'}
CHUNKSIZE = 500_000


class SearchKeywordProcessor:
    """Chunked processing for large files."""

    def process(self, input_file, output_file):
        # Pass 1: Find first search referral per IP
        first_search = {}

        for chunk in pd.read_csv(input_file, sep='\t', usecols=USECOLS, dtype=DTYPES, chunksize=CHUNKSIZE):
            chunk['domain'] = chunk['referrer'].str.extract(r'https?://([^/]+)', expand=False)
            chunk['keyword'] = (
                chunk['referrer']
                .str.extract(r'[?&][qp]=([^&]+)', expand=False)
                .str.replace('+', ' ', regex=False)
                .str.replace('%20', ' ', regex=False)
                .str.lower()
            )
            search_hits = chunk.loc[chunk['domain'].isin(SEARCH_ENGINES), ['ip', 'domain', 'keyword']]

            for _, row in search_hits.iterrows():
                if row['ip'] not in first_search:
                    first_search[row['ip']] = (row['domain'], row['keyword'])

        # Pass 2: Aggregate revenue
        revenue = {}
        total_rows = 0
        total_purchases = 0

        for chunk in pd.read_csv(input_file, sep='\t', usecols=USECOLS, dtype=DTYPES, chunksize=CHUNKSIZE):
            total_rows += len(chunk)

            chunk['is_purchase'] = chunk['event_list'].str.contains(r'(?:^|,)1(?:,|$)', na=False, regex=True)
            chunk['revenue'] = (
                chunk['product_list']
                .str.extract(r'^[^;]*;[^;]*;[^;]*;([^;,]*)', expand=False)
                .pipe(pd.to_numeric, errors='coerce')
                .fillna(0)
            )

            purchases = chunk.loc[(chunk['is_purchase']) & (chunk['revenue'] > 0)]

            for _, row in purchases.iterrows():
                if row['ip'] in first_search:
                    key = first_search[row['ip']]
                    revenue[key] = revenue.get(key, 0) + row['revenue']
                    total_purchases += 1

        # Convert to DataFrame
        result = pd.DataFrame([
            {'Search Engine Domain': k[0], 'Search Keyword': k[1], 'Revenue': v}
            for k, v in revenue.items()
        ])
        print(result)

        if len(result) > 0:
            result = result.sort_values('Revenue', ascending=False)

        result.to_csv(output_file, sep='\t', index=False, float_format='%.2f')

        return {
            'rows_processed': total_rows,
            'purchases_found': total_purchases,
            'unique_keywords': len(result),
            'total_revenue': float(result['Revenue'].sum()) if len(result) > 0 else 0.0
        }


def lambda_handler(event, context):
    print("Testing NEw lambda")
    print(f"Event: {json.dumps(event)}")

    try:
        if 'Records' in event:
            results = []
            for record in event['Records']:
                bucket = record['s3']['bucket']['name']
                key = unquote_plus(record['s3']['object']['key'])
                results.append(process_s3_file(bucket, key))
            return {'statusCode': 200, 'body': json.dumps({'results': results})}

        elif 'input_bucket' in event:
            result = process_s3_file(
                event['input_bucket'],
                event['input_key'],
                event.get('output_bucket')
            )
            return {'statusCode': 200, 'body': json.dumps({'result': result})}

        else:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid event'})}

    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def process_s3_file(input_bucket, input_key, output_bucket=None):
    output_bucket = output_bucket or input_bucket

    with tempfile.NamedTemporaryFile(delete=False, suffix='.tsv') as tmp_in, \
         tempfile.NamedTemporaryFile(delete=False, suffix='.tab') as tmp_out:

        s3_client.download_file(input_bucket, input_key, tmp_in.name)

        processor = SearchKeywordProcessor()
        stats = processor.process(tmp_in.name, tmp_out.name)

        output_key = f"output/{datetime.now().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"
        s3_client.upload_file(tmp_out.name, output_bucket, output_key)

        os.unlink(tmp_in.name)
        os.unlink(tmp_out.name)

        return {
            'input': f"s3://{input_bucket}/{input_key}",
            'output': f"s3://{output_bucket}/{output_key}",
            **stats
        }
