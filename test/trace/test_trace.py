import unittest
from sqlgraph.trace import SqlTrace
from test.dialect import PostgresExtended
from sqlgraph.schema import DictSchema


class TraceTests(unittest.TestCase):
    
    def test_sql_sourced_schema(self):
        SQLs = {
            'table_1': """
              SELECT
                NULL::TEXT as col_1,
                NULL::BOOLEAN as col_2
            """,
            'table_2': """
              SELECT
                *
              FROM table_1
            """
        }
        
        t = SqlTrace.trace_sql(SQLs, dialect=PostgresExtended, schema=DictSchema({}))
        print(t)
    
    def test_struct_source(self):
        TABLES = {
            'test_db': {
                'test_schema': {
                    'name_table': [
                        'person_id',
                        'first_name',
                        'last_name',
                    ]
                }
            }
        }
        
        SQL = """\
          WITH names AS (
            SELECT
              JSON_BUILD_OBJECT(
                'first_name', first_name,
                'last_name', last_name
              ) as name
            FROM name_table
          )
          SELECT
            name->'first_name' as fn,
            name->'last_name' as ln
          FROM names
        """

        t = SqlTrace.trace_sql(SQL, 'name_test', dialect=PostgresExtended, schema=DictSchema(TABLES))
        ts = t.tables['name_test']
        for column_name, column_source in ts.sources.items():
            print(f'{column_name}: {column_source}')
        print(ts)
        print(t)
    
    def test_chained_json_get(self):
        TABLES = {}
        
        SQL = """\
          --SELECT COALESCE(j->'SCREENING DEPTH'->>'os') AS screening_depth_os
          SELECT COALESCE('a', 'b') AS screening_depth_os
          FROM (SELECT '{"a": {"b": 1}}'::JSON as j);
        """

        t = SqlTrace.trace_sql(SQL, 'test_chained_json_get', dialect=PostgresExtended, schema=DictSchema(TABLES))
        ts = t.tables['test_chained_json_get']
        for column_name, column_source in ts.sources.items():
            print(f'{column_name}: {column_source}')
        print(ts)
        print(t)
        
        
        