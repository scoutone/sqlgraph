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
        
        traced = SqlTrace.trace_sql(SQLs, dialect='postgres', schema=schema)
        
        for table, columns in traced.tables.items():
            print(table)
            for field, source in columns.items():
                print(f'{field}: {json.dumps(source.to_dict(), indent=2)}')
                print()
            
        g = traced.to_graph(include_intermediate_tables=True)
        

        A = g.to_agraph()        
        A.draw("test.png", prog="dot")
        
        