"""
Unit tests for Search Keyword Performance Analyzer
"""

import unittest
import tempfile
import os
import pandas as pd
from search_keyword_performance import SearchKeywordProcessor


class TestSearchKeywordProcessor(unittest.TestCase):
    """Test cases for SearchKeywordProcessor class."""

    def setUp(self):
        """Create sample data file for testing."""
        self.sample_data = """hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27 06:34:40\tMozilla/5.0\t67.98.123.1\t\tSalem\tOR\tUS\tHome\thttp://www.esshopzilla.com\t\thttp://www.google.com/search?q=Ipod
1254033379\t2009-09-27 06:36:19\tMozilla/5.0\t23.8.61.21\t2\tRochester\tNY\tUS\tZune\thttp://www.esshopzilla.com/product/\tElectronics;Zune;1;;\thttp://www.bing.com/search?q=Zune
1254034666\t2009-09-27 06:57:46\tMozilla/5.0\t23.8.61.21\t1\tRochester\tNY\tUS\tOrder Complete\thttp://www.esshopzilla.com/checkout/\tElectronics;Zune - 32GB;1;250;\thttp://www.esshopzilla.com/checkout/
1254035260\t2009-09-27 07:07:40\tMozilla/5.0\t67.98.123.1\t1\tSalem\tOR\tUS\tOrder Complete\thttp://www.esshopzilla.com/checkout/\tElectronics;Ipod - Touch;1;290;\thttp://www.esshopzilla.com/checkout/"""

        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv')
        self.temp_file.write(self.sample_data)
        self.temp_file.close()

    def tearDown(self):
        """Clean up temp file."""
        os.unlink(self.temp_file.name)

    def test_process_returns_dataframe(self):
        """Test that process() returns a DataFrame."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        self.assertIsInstance(result, pd.DataFrame)

    def test_correct_columns(self):
        """Test that output has correct columns."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        expected_columns = ['Search Engine Domain', 'Search Keyword', 'Revenue']
        self.assertListEqual(list(result.columns), expected_columns)

    def test_correct_revenue_calculation(self):
        """Test that revenue is calculated correctly."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        total_revenue = result['Revenue'].sum()
        self.assertEqual(total_revenue, 540.0)  # 250 + 290

    def test_sorted_by_revenue_descending(self):
        """Test that results are sorted by revenue descending."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        revenues = result['Revenue'].tolist()
        self.assertEqual(revenues, sorted(revenues, reverse=True))

    def test_google_keyword_attribution(self):
        """Test Google keyword revenue attribution."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        google_row = result[result['Search Engine Domain'] == 'www.google.com']
        self.assertEqual(google_row['Revenue'].values[0], 290.0)
        self.assertEqual(google_row['Search Keyword'].values[0], 'ipod')

    def test_bing_keyword_attribution(self):
        """Test Bing keyword revenue attribution."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        result = processor.process()
        bing_row = result[result['Search Engine Domain'] == 'www.bing.com']
        self.assertEqual(bing_row['Revenue'].values[0], 250.0)
        self.assertEqual(bing_row['Search Keyword'].values[0], 'zune')

    def test_get_stats(self):
        """Test get_stats() returns correct statistics."""
        processor = SearchKeywordProcessor(self.temp_file.name)
        processor.process()
        stats = processor.get_stats()

        self.assertEqual(stats['rows_processed'], 4)
        self.assertEqual(stats['purchases_found'], 2)
        self.assertEqual(stats['unique_keywords'], 2)
        self.assertEqual(stats['total_revenue'], 540.0)

    def test_non_search_engine_referrer_ignored(self):
        """Test that internal referrers are ignored."""
        data = """hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27\tMozilla\t1.1.1.1\t\tCity\tST\tUS\tHome\thttp://site.com\t\thttp://www.esshopzilla.com/page
1254033380\t2009-09-27\tMozilla\t1.1.1.1\t1\tCity\tST\tUS\tComplete\thttp://site.com\tCat;Prod;1;100;\thttp://www.esshopzilla.com/checkout"""

        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv')
        temp.write(data)
        temp.close()

        try:
            processor = SearchKeywordProcessor(temp.name)
            result = processor.process()
            self.assertEqual(len(result), 0)  # No search engine, so no results
        finally:
            os.unlink(temp.name)

    def test_no_purchase_no_revenue(self):
        """Test that users without purchases don't contribute revenue."""
        data = """hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27\tMozilla\t1.1.1.1\t\tCity\tST\tUS\tHome\thttp://site.com\t\thttp://www.google.com/search?q=test
1254033380\t2009-09-27\tMozilla\t1.1.1.1\t2\tCity\tST\tUS\tProduct\thttp://site.com\tCat;Prod;1;100;\thttp://site.com"""

        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv')
        temp.write(data)
        temp.close()

        try:
            processor = SearchKeywordProcessor(temp.name)
            result = processor.process()
            self.assertEqual(len(result), 0)  # No purchase event (1), so no revenue
        finally:
            os.unlink(temp.name)

    def test_keyword_extraction_with_plus_sign(self):
        """Test keyword extraction handles + signs correctly."""
        data = """hit_time_gmt\tdate_time\tuser_agent\tip\tevent_list\tgeo_city\tgeo_region\tgeo_country\tpagename\tpage_url\tproduct_list\treferrer
1254033280\t2009-09-27\tMozilla\t1.1.1.1\t\tCity\tST\tUS\tHome\thttp://site.com\t\thttp://search.yahoo.com/search?p=cd+player
1254033380\t2009-09-27\tMozilla\t1.1.1.1\t1\tCity\tST\tUS\tComplete\thttp://site.com\tCat;Prod;1;50;\thttp://site.com"""

        temp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv')
        temp.write(data)
        temp.close()

        try:
            processor = SearchKeywordProcessor(temp.name)
            result = processor.process()
            self.assertEqual(result['Search Keyword'].values[0], 'cd player')
        finally:
            os.unlink(temp.name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
