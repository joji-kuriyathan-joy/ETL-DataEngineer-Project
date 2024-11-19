import unittest
from etl-pipeline import DBConnection

class TestDBConnection(unittest.TestCase):
    def setUp(self):
        self.db_config = {
            'host': 'localhost',
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db'
        }
        self.db_connection = DBConnection('mysql', self.db_config)

if __name__ == '__main__':
    unittest.main()
