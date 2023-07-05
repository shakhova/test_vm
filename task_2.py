import apache_beam as beam
import unittest
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from apache_beam.testing.test_pipeline import TestPipeline
from pathlib import Path
p = Path.cwd()

class ConvertStringToDate(beam.DoFn):
    def process(self, element):
        try:
            date_str = element['date']
            date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %Z').date()
            element['date'] = date_obj
            yield element
        except ValueError:
            # Skip invalid dates or handle them as per your requirement
            pass

class FilterTransactions(beam.DoFn):
    def process(self, element):
        transaction_date = element['date']
        transaction_amount = element['transaction_amount']
        if transaction_amount > 20 and transaction_date.year >= 2010:
            yield element


class SumByDate(beam.DoFn):
    def process(self, element):
        yield (element['date'].strftime('%Y-%m-%d'), element['transaction_amount'])


class CompositeTransform(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Parse lines' >> beam.Map(lambda line: line.split(','))
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
        )


class TestCompositeTransform(unittest.TestCase):
    def test_composite_transform(self):
        
        input_data = '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99\n\
        2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95\n\
        2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22\n\
        2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030\n\
        2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08'


        expected_output = [{'date': '2017-03-18', 'total_amount': 2102.22},
        {'date': '2017-08-31', 'total_amount': 13700000023.08},
        {'date': '2018-02-27', 'total_amount': 129.12}]

        with TestPipeline() as p:
            
            input_pcoll = p | beam.Create(input_data)

            output_pcoll = input_pcoll | CompositeTransform()

            beam.testing.assert_that(output_pcoll, beam.testing.equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
