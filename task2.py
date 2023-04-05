import argparse
import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.options.pipeline_options import PipelineOptions

from sum_transactions import SumByDay


def run(argv=None):
    '''
        Construct and execute ApacheBeam pipeline for task 2
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='myproject')
    parser.add_argument('--runner', default='DirectRunner')
    parser.add_argument('--temp_location', default='/temp')
    parser.add_argument('--jobname', default='DETT_ProcessTransactions')
    parser.add_argument('--input', default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')
    parser.add_argument('--output', default='output/task2-results.jsonl.gz')
    parser.add_argument('--skip_header_lines', default=True)
    parser.add_argument('--column_delimiter', default=',')
    parser.add_argument('--column_date_format', default='%Y-%m-%d', help='Opportunity to override dateformat in source file.')
    parser.add_argument('--column_index_date', default=0)
    parser.add_argument('--column_index_amount', default=3)
    parser.add_argument('--filter_date_from', default='2011-01-01', help='Enter in format: YYYY-MM-DD.')
    parser.add_argument('--filter_min_value', default=0)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline 
            | 'ReadFromFile' >> beam.io.ReadFromText(known_args.input, skip_header_lines=known_args.skip_header_lines)
            | SumByDay(known_args=known_args)
            | 'WriteResults' >> beam.io.WriteToText(file_path_prefix=known_args.output,
                                                num_shards=0, shard_name_template='', compression_type=filesystem.CompressionTypes.AUTO)
        )


if __name__ == '__main__':
    run()
