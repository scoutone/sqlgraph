import unittest
from sqlgraph.trace import SqlTrace
from test.dialect import PostgresExtended
from sqlgraph.schema import DictSchema


class TraceTests(unittest.TestCase):
    
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
        
        
        