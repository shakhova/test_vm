import apache_beam as beam
import unittest

from datetime import datetime
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FilterTransactions(beam.DoFn):
    def process(self, element):
        transaction_date = datetime.strptime(element['date'], '%Y-%m-%d').date()
        transaction_amount = element['transaction_amount']
        if transaction_amount > 20 and transaction_date.year >= 2010:
            yield element


class SumByDate(beam.DoFn):
    def process(self, element):
        yield (element['date'], float(element['transaction_amount']))


class CompositeTransform(beam.PTransform):
    def expand(self, pcoll):

        return (
            pcoll
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

        input_data = [
        {'date':'2009-01-09','transaction_amount':1021101.99},
        {'date':'2017-01-01','transaction_amount':19.95},
        {'date':'2017-03-18','transaction_amount':2102.22},
        {'date':'2017-03-18','transaction_amount':1.00030},
        {'date':'2017-08-31','transaction_amount':13700000023.08}]


        expected_output = [{'date': '2017-03-18', 'total_amount': 2102.22},
        {'date': '2017-08-31', 'total_amount': 13700000023.08}]

        with TestPipeline() as p:
            
            input_pcoll = p | beam.Create(input_data)

            output_pcoll = input_pcoll | CompositeTransform()

            assert_that(output_pcoll, equal_to(expected_output))


if __name__ == '__main__':
    unittest.main()
