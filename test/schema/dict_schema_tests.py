from sqlgraph.schema import DictSchema, Table
import unittest


class DictSchemaTests(unittest.TestCase):
    def test_fully_qualified(self):
        schema = DictSchema(
            {
                'test_catalog': {
                    'test_db': {
                        'test_table': [
                            'column1',
                            'column2'
                        ]
                    }
                }
            }
        )

        self.assertEqual(
            Table('test_table', ['column1', 'column2'], 'test_db', 'test_catalog'), 
            schema.get_table('test_table', 'test_db', 'test_catalog')
        )
        
    def test_table_only__single_match(self):
        schema = DictSchema(
            {
                'test_catalog1': {
                    'test_db': {
                        'test_table': [
                            'column1',
                            'column2'
                        ],
                        'test_table2': [
                            'column1',
                            'column2'
                        ],
                    }
                },
                'test_catalog2': {
                    'test_db': {
                        'test_table': [
                            'column1',
                            'column2'
                        ],
                        'test_table3': [
                            'column1',
                            'column2'
                        ],
                    }
                }
            }
        )
        
        self.assertEqual(
            Table('test_table2', ['column1', 'column2'], 'test_db', 'test_catalog1'),
            schema.get_table('test_table2')
        )
        
    def test_table_only__duplicate_match(self):
        schema = DictSchema(
            {
                'test_catalog1': {
                    'test_db': {
                        'test_table': [
                            'column1',
                            'column2'
                        ],
                        'test_table2': [
                            'column1',
                            'column2'
                        ],
                    }
                },
                'test_catalog2': {
                    'test_db': {
                        'test_table': [
                            'column1',
                            'column2'
                        ],
                        'test_table3': [
                            'column1',
                            'column2'
                        ],
                    }
                }
            }
        )
        
        with self.assertRaises(ValueError):
            self.assertEqual(
                ['column1', 'column2'], 
                schema.get_table('test_table')
            )