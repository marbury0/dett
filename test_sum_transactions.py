import argparse
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that, equal_to

from sum_transactions import SumByDay
import unittest


class TestMyModule(unittest.TestCase):
    def test_process_data(self):

        parser = argparse.ArgumentParser()
        parser.add_argument('--project', default='myproject')
        parser.add_argument('--runner', default='DirectRunner')
        parser.add_argument('--temp_location', default='/temp')
        parser.add_argument('--jobname', default='DETT_ProcessTransactions')
        parser.add_argument('--input', default='tests/test.csv')
        parser.add_argument('--output', default='output/task2-results.jsonl')
        parser.add_argument('--skip_header_lines', default=True)
        parser.add_argument('--column_delimiter', default=',')
        parser.add_argument('--column_date_format', default='%Y-%m-%d', help='Opportunity to override dateformat in source file.')
        parser.add_argument('--column_index_date', default=0)
        parser.add_argument('--column_index_amount', default=3)
        parser.add_argument('--filter_date_from', default='2011-01-01', help='Enter in format: YYYY-MM-DD.')
        parser.add_argument('--filter_min_value', default=0)

        known_args, pipeline_args = parser.parse_known_args()

        expected_output = [[{'date': '2023-03-01', 'total_amount': 20.00}]]

        pipeline_options = PipelineOptions(pipeline_args)
        with TestPipeline(options=pipeline_options) as pipeline:
            output = (
                pipeline 
                | 'ReadFromFile' >> beam.io.ReadFromText(known_args.input, skip_header_lines=known_args.skip_header_lines)
                | SumByDay(known_args=known_args)
            )
            assert_that(output, equal_to(expected_output))
