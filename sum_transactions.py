import datetime
import apache_beam as beam

@beam.ptransform_fn
def SumByDay(pcoll, known_args):
  return (
      pcoll
        | 'SplitByDelimeter' >> beam.Map(lambda line: line.split(known_args.column_delimiter))
        | 'FilterByFromDate' >> beam.Filter(lambda x:
                                                datetime.datetime.strptime((x[known_args.column_index_date][:10]),
                                                                           known_args.column_date_format) >= datetime.datetime.strptime(known_args.filter_date_from, '%Y-%m-%d'))
        | 'FilterByAmount' >> beam.Filter(lambda x: (float)(x[known_args.column_index_amount]) >= (float)(known_args.filter_min_value))
        | 'ToTuple' >> beam.Map(lambda x: (x[known_args.column_index_date][:10], (float)(x[known_args.column_index_amount])))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'TupleToJson' >> beam.Map(lambda x: ([{'date': x[0], 'total_amount': x[1]}]))
  )