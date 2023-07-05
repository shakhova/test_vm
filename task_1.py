import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime


class FilterTransactions(beam.DoFn):
    def process(self, element):
        transaction_date = element['date']
        transaction_amount = element['transaction_amount']
        if transaction_amount > 20 and transaction_date.year >= 2010:
            yield element


class SumByDate(beam.DoFn):
    def process(self, element):
        yield (element['date'].strftime('%Y-%m-%d'), element['transaction_amount'])


def run():
    input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
    output_file = 'output/results'

    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        transactions = (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse CSV' >> beam.Map(lambda line: line.split(','))
            | 'Create Dict' >> beam.Map(lambda fields: {
                'date': datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S %Z').date(),
                'transaction_amount': float(fields[3])
            })
            | 'Filter Transactions' >> beam.ParDo(FilterTransactions())
            | 'Sum Transactions by Date' >> beam.ParDo(SumByDate())
            | 'Group by Date' >> beam.GroupByKey()
            | 'Sum Amounts' >> beam.Map(lambda item: {
                'date': item[0],
                'total_amount': sum(item[1])
            })
            | 'Write Output' >> beam.io.WriteToText(output_file, file_name_suffix='.jsonl.gz')
        )


if __name__ == '__main__':
    run()