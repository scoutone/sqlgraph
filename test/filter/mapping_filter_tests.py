import unittest
from sqlgraph.schema import DictSchema
from sqlgraph.trace import SqlTrace
from test.dialect import PostgresExtended
from sqlgraph.filter import SimpleFilter
import json


class MappingFilterTests(unittest.TestCase):
    def test_source_mapping(self):
        TABLES = {
            'test_db': {
                'public': {
                    'default_table_a': [
                        'field_1',
                        'field_2'
                    ]
                },
                'schema_1': {
                    'source_table_b': [
                        'field_b1',
                        'field_b2'
                    ],
                    'source_table_c': [
                        'field_c1',
                        'field_c2'
                    ],
                    'intermediate_table_c': [
                        'constant_field',
                        'c1',
                        'c2'
                    ]  ,
                    'dest_table_b': [
                        'constant_field',
                        'b1',
                        'b2'
                    ]    
                }
            }
        }
        
        SQLs = {
            'dest_table_a': """\
                SELECT
                  *
                FROM public.default_table_a
                LIMIT 0
            """,
            'dest_table_b': """\
                SELECT
                    NULL::TEXT as constant_field,
                    field_b1 AS b1,
                    field_b2 AS b2
                FROM schema_1.source_table_b
            """,
            'intermediate_table_c': """\
                SELECT
                    'SOME_VALUE' AS constant_field,
                    field_c1 || 'MORE_TEXT' AS c1,
                    field_c2 AS c2
                FROM schema_1.source_table_c
            """,
            'dest_table_c': """\
                SELECT
                    *
                FROM schema_1.intermediate_table_c
            """,
            'dest_table_d': """\
                SELECT
                    *
                FROM schema_1.intermediate_table_c
            """,
            'dest_table_e': """\
                SELECT
                    *
                FROM schema_1.dest_table_b
            """
        }
        
        schema = DictSchema(TABLES) 
        traced = SqlTrace.trace_sql(SQLs, dialect=PostgresExtended, schema=schema, catalog='test_db', db='schema_1')
        print(traced)
        
        g = traced.to_graph()
        g.to_agraph().draw("test_source_mapping_before.png", prog="dot") 
        
        dest_tables=[
                'test_db.schema_1.dest_table_a', 
                'test_db.schema_1.dest_table_b', 
                'test_db.schema_1.dest_table_c', 
                'test_db.schema_1.dest_table_d'
            ]
        
        
        f = SimpleFilter(
            dest_tables=dest_tables,
            excluded_tables=[
                'test_db.schema_1.intermediate_table_c',
                'test_db.public.default_table_a'
            ]
        )
        filtered = f.apply(g)
        print(json.dumps(filtered.to_dict(simple=True), indent=2))

        A = filtered.to_agraph()  
        A.draw("test_source_mapping.png", prog="dot") 