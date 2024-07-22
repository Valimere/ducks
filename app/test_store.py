import unittest
from store import DuckDBStore


class TestDuckDBStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = DuckDBStore()
        cls.store.connection.execute("""
            CREATE TABLE billing_data (
                Line_item_unblended_cost DOUBLE,
                Product_servicecode VARCHAR,
                Line_item_line_item_type VARCHAR
            );
        """)
        cls.store.connection.execute("""
            INSERT INTO billing_data VALUES
            (10.0, 'AmazonS3', 'Usage'),
            (20.0, 'AmazonEC2', 'Usage'),
            (30.0, 'AWSDataTransfer', 'Usage'),
            (40.0, 'AWSGlue', 'Usage'),
            (50.0, 'AmazonGuardDuty', 'Usage'),
            (60.0, 'AmazonS3', 'NotUsage');
        """)

    def test_query_undiscounted_cost(self):
        cost = self.store.query_undiscounted_cost('AmazonS3')
        self.assertEqual(cost, 10.0)

    def test_query_discounted_cost(self):
        cost = self.store.query_discounted_cost('AmazonS3', 0.88)
        self.assertEqual(cost, 8.8)

    def test_query_blended_discount_rate(self):
        rate = self.store.query_blended_discount_rate()
        self.assertAlmostEqual(rate, 0.5375)

if __name__ == '__main__':
    unittest.main()
