import unittest
import json
from sqlgraph.trace import SqlTrace
import networkx as nx
import time
from networkx.drawing.nx_agraph import graphviz_layout
from sqlgraph.schema import DictSchema
 
class LayoutTest(unittest.TestCase):
  
    def test_layout(self):
        TABLES = {
            'test_db': {
                'test_schema': {
                    'person': [
                        'person_id',
                        'name_first',
                        'name_last',
                    ],
                    'address': [
                        'person_id',
                        'address',
                        'city',
                        'state',
                        'zip'
                    ],
                    ##TODO: GET THIS FROM OTHER SQL
                    'named_address': [
                        'name_first',
                        'name_last',
                        'address',
                        'city',
                        'state',
                        'zip'
                    ]
                }
            }
        }
        SQLs = {
            'named_address': """\
              SELECT
                name_first,
                name_last,
                address,
                city,
                state,
                zip
              FROM test_db.test_schema.person p
              JOIN test_db.test_schema.address a
                ON p.person_id = a.person_id
            """,
            'formatted_address': """\
              SELECT
                name_first || ' ' || name_last AS name,
                address || ' ' || city || ' ' || state || ' ' || zip as address
              FROM test_db.test_schema.named_address
            """
        }

        schema = DictSchema(TABLES)
        
        traced = SqlTrace.trace_sql(SQLs, dialect='postgres', schema=schema, db='test_schema', catalog='test_db')
        
        actual = {
            table_name: table_source.to_dict()
            for table_name, table_source in traced.tables.items()
        }
        
        print(json.dumps(actual, indent=2))
            
        g = traced.to_graph()

        A = g.to_agraph()        
        A.draw("test_layout.png", prog="dot")
        
    def test_layout_union(self):
        TABLES = {
            None: {
                None: {
                    'cats': [
                        'name',
                        'age'
                    ],
                    'dogs': [
                        'name',
                        'age'
                    ],
                    'rats': [
                        'name',
                        'age'
                    ]
                }
            }
        }
        SQLs = {
            'pets': """\
              SELECT 'cat' as type, * FROM cats
              UNION ALL
              SELECT 'dog' as type, * FROM dogs
              UNION ALL
              SELECT 'rat' as type, * FROM rats
            """
        }
        
        schema = DictSchema(TABLES)
        traced = SqlTrace.trace_sql(SQLs, dialect='postgres', schema=schema)
        
        actual = {
            table_name: table_source.to_dict()
            for table_name, table_source in traced.tables.items()
        }
        
        print(json.dumps(actual, indent=2))
            
        g = traced.to_graph()

        A = g.to_agraph()        
        A.draw("test_layout_union.png", prog="dot")
        
        