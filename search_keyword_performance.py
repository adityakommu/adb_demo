"""
Search Keyword Performance - Chunked Processing

Handles large files (10GB+) using two-pass chunked processing.
"""

import argparse
import pandas as pd
from datetime import datetime


class SearchKeywordProcessor:
    """Processes hit-level data to analyze search keyword revenue performance."""

    SEARCH_ENGINES = ['google.com', 'www.google.com', 'bing.com', 'www.bing.com',
                      'search.yahoo.com', 'yahoo.com', 'www.yahoo.com', 'msn.com']
    USECOLS = ['ip', 'referrer', 'event_list', 'product_list']
    DTYPES = {'ip': 'string', 'referrer': 'string', 'event_list': 'string', 'product_list': 'string'}
    CHUNKSIZE = 500_000

    def __init__(self, input_file):
        self.input_file = input_file
        self.first_search = {}
        self.revenue = {}
        self.total_rows = 0
        self.total_purchases = 0

    def _extract_search_info(self, chunk):
        """Extract domain and keyword from referrer."""
        chunk['domain'] = chunk['referrer'].str.extract(r'https?://([^/]+)', expand=False)
        chunk['keyword'] = (
            chunk['referrer']
            .str.extract(r'[?&][qp]=([^&]+)', expand=False)
            .str.replace('+', ' ', regex=False)
            .str.replace('%20', ' ', regex=False)
            .str.lower()
        )
        return chunk

    def _extract_revenue(self, chunk):
        """Extract purchase flag and revenue from chunk."""
        chunk['is_purchase'] = chunk['event_list'].str.contains(r'(?:^|,)1(?:,|$)', na=False, regex=True)
        chunk['revenue'] = (
            chunk['product_list']
            .str.extract(r'^[^;]*;[^;]*;[^;]*;([^;,]*)', expand=False)
            .pipe(pd.to_numeric, errors='coerce')
            .fillna(0)
        )
        return chunk

    def _pass1_find_search_referrals(self):
        """Pass 1: Find first search referral per IP."""
        print("Pass 1: Finding first search referrals...")

        for chunk in pd.read_csv(self.input_file, sep='\t', usecols=self.USECOLS,
                                  dtype=self.DTYPES, chunksize=self.CHUNKSIZE):
            chunk = self._extract_search_info(chunk)
            search_hits = chunk.loc[chunk['domain'].isin(self.SEARCH_ENGINES), ['ip', 'domain', 'keyword']]

            for _, row in search_hits.iterrows():
                if row['ip'] not in self.first_search:
                    self.first_search[row['ip']] = (row['domain'], row['keyword'])

        print(f"  Found {len(self.first_search)} users from search engines")

    def _pass2_aggregate_revenue(self):
        """Pass 2: Aggregate revenue by search keyword."""
        print("Pass 2: Aggregating revenue...")

        for chunk in pd.read_csv(self.input_file, sep='\t', usecols=self.USECOLS,
                                  dtype=self.DTYPES, chunksize=self.CHUNKSIZE):
            self.total_rows += len(chunk)
            chunk = self._extract_revenue(chunk)
            purchases = chunk.loc[(chunk['is_purchase']) & (chunk['revenue'] > 0)]

            for _, row in purchases.iterrows():
                if row['ip'] in self.first_search:
                    key = self.first_search[row['ip']]
                    self.revenue[key] = self.revenue.get(key, 0) + row['revenue']
                    self.total_purchases += 1

        print(f"  Processed {self.total_rows:,} rows, {self.total_purchases} purchases")

    def process(self):
        """Process file using two-pass chunked approach."""
        self._pass1_find_search_referrals()
        self._pass2_aggregate_revenue()

        result = pd.DataFrame([
            {'Search Engine Domain': k[0], 'Search Keyword': k[1], 'Revenue': v}
            for k, v in self.revenue.items()
        ])
        print("REsult is :")
        print(result)

        return result.sort_values('Revenue', ascending=False) if len(result) > 0 else result

    def get_stats(self):
        """Return processing statistics."""
        return {
            'rows_processed': self.total_rows,
            'purchases_found': self.total_purchases,
            'unique_keywords': len(self.revenue),
            'total_revenue': sum(self.revenue.values())
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('-o', '--output', default=None)
    args = parser.parse_args()

    processor = SearchKeywordProcessor(args.input_file)

    result = processor.process()

    output_file = args.output or f"{datetime.now().strftime('%Y-%m-%d')}_SearchKeywordPerformance.tab"
    result.to_csv(output_file, sep='\t', index=False, float_format='%.2f')
    
    print(f"\n{'='*50}")
    print(result.to_string(index=False))
    print(f"\nTotal Revenue: ${result['Revenue'].sum():,.2f}")
    print(f"Output: {output_file}")


if __name__ == '__main__':
    main()
