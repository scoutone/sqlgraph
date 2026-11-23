import os
import tempfile
import unittest

from sqlgraph.graph import SqlGraph
from sqlgraph.trace import SqlTrace
from test.trace.helpers import TraceTestCase, trace_all, col, UNKNOWN, resolve, SCHEMA, DIALECT


class ModelTraceTests(TraceTestCase):
    """Tracing several models that reference each other, and the SqlTrace API around them"""

    # ---- model references ------------------------------------------------------------------------

    def test_model_references_another_model(self):
        t = trace_all({
            'a': "SELECT first_name AS fn FROM db.people",
            'b': "SELECT fn FROM a",
        })
        self.assertEqual(['a', 'b'], sorted(t.tables))
        self.assert_source(t.table('b'), 'fn', col('people', 'first_name'))

    def test_model_reference_order_does_not_matter(self):
        t = trace_all({
            'b': "SELECT fn FROM a",
            'a': "SELECT first_name AS fn FROM db.people",
        })
        self.assert_source(t.table('b'), 'fn', col('people', 'first_name'))

    def test_model_chain(self):
        t = trace_all({
            'a': "SELECT first_name AS fn FROM db.people",
            'b': "SELECT fn AS f2 FROM a",
            'c': "SELECT f2 FROM b",
        })
        self.assert_source(t.table('c'), 'f2', col('people', 'first_name'))

    def test_model_star(self):
        t = trace_all({
            'a': "SELECT first_name AS fn, age FROM db.people",
            'b': "SELECT * FROM a",
        })
        self.assert_columns(t.table('b'), ['fn', 'age'])
        self.assert_source(t.table('b'), 'fn', col('people', 'first_name'))

    def test_model_joined_with_table(self):
        t = trace_all({
            'a': "SELECT id AS aid FROM db.people",
            'b': "SELECT a.aid, d.name FROM a JOIN db.depts d ON d.id = a.aid",
        })
        self.assert_sources(t.table('b'), {'aid': col('people', 'id'), 'name': col('depts', 'name')})

    def test_model_referenced_by_two_models(self):
        t = trace_all({
            'base': "SELECT id, age FROM db.people",
            'x': "SELECT id FROM base",
            'y': "SELECT age FROM base",
        })
        self.assert_source(t.table('x'), 'id', col('people', 'id'))
        self.assert_source(t.table('y'), 'age', col('people', 'age'))

    def test_model_with_union_source(self):
        t = trace_all({
            'u': "SELECT id AS v FROM db.people UNION ALL SELECT id AS v FROM db.depts",
            'w': "SELECT v FROM u",
        })
        self.assertEqual(
            ('union', [col('people', 'id'), col('depts', 'id')]),
            resolve(t.table('w').sources['v'])
        )

    def test_model_name_shadows_schema_table(self):
        # a traced model takes precedence over a table of the same name in the schema
        t = trace_all({
            'depts': "SELECT first_name AS name FROM db.people",
            'x': "SELECT name FROM depts",
        })
        self.assert_source(t.table('x'), 'name', col('people', 'first_name'))

    def test_model_lateral_over_another_model(self):
        t = trace_all({
            'src': "SELECT id, data FROM db.people",
            'x': "SELECT l.v FROM src s CROSS JOIN LATERAL (SELECT s.id AS v) l",
        })
        self.assert_source(t.table('x'), 'v', col('people', 'id'))

    # ---- identical SQL ---------------------------------------------------------------------------
    # sqlglot expressions compare structurally, so the tracer must not treat identical SQL as the same node

    def test_models_with_identical_sql_stay_distinct(self):
        t = trace_all({
            'x': "SELECT first_name FROM db.people",
            'y': "SELECT first_name FROM db.people",
        })
        self.assertEqual(['x', 'y'], sorted(table.name for table in t.tables.values()))
        self.assertIsNot(t.table('x'), t.table('y'))
        self.assert_source(t.table('x'), 'first_name', col('people', 'first_name'))
        self.assert_source(t.table('y'), 'first_name', col('people', 'first_name'))

    def test_models_with_identical_star_sql_stay_distinct(self):
        t = trace_all({
            'x': "SELECT * FROM db.people",
            'y': "SELECT * FROM db.people",
            'z': "SELECT * FROM db.people",
        })
        self.assertEqual(['x', 'y', 'z'], sorted(table.name for table in t.tables.values()))

    def test_models_with_identical_sql_all_reach_the_graph(self):
        t = trace_all({
            'x': "SELECT first_name FROM db.people",
            'y': "SELECT first_name FROM db.people",
        })
        g = t.to_graph().g
        for model in ['x', 'y']:
            self.assertIn(f'{model}.first_name', g.nodes)
            self.assertEqual({'cat.db.people.first_name'}, {src for src, _ in g.in_edges(f'{model}.first_name')})

    def test_models_with_identical_sql_referencing_another_model(self):
        t = trace_all({
            'base': "SELECT first_name AS fn FROM db.people",
            'x': "SELECT fn FROM base",
            'y': "SELECT fn FROM base",
        })
        self.assertEqual(['base', 'x', 'y'], sorted(table.name for table in t.tables.values()))
        self.assert_source(t.table('y'), 'fn', col('people', 'first_name'))

    def test_identical_derived_tables_in_one_model_are_traced_separately(self):
        ts = trace_all("""
            SELECT a.v AS av, b.v AS bv
            FROM (SELECT age AS v FROM db.people) a
            JOIN (SELECT age AS v FROM db.people) b ON a.v = b.v
        """, 'm').table('m')
        a_table, b_table = ts.sources['av'].table, ts.sources['bv'].table
        self.assertIsNot(a_table, b_table)
        self.assertNotEqual(a_table.name, b_table.name)
        self.assert_source(ts, 'av', col('people', 'age'))
        self.assert_source(ts, 'bv', col('people', 'age'))

    def test_same_cte_referenced_twice_is_still_traced_once(self):
        # identity based caching must still share work for the very same node
        ts = trace_all("""
            WITH c AS (SELECT age AS v FROM db.people)
            SELECT a.v AS av, b.v AS bv FROM c a JOIN c b ON a.v = b.v
        """, 'm').table('m')
        self.assertIs(ts.sources['av'].table, ts.sources['bv'].table)

    # ---- filtering -------------------------------------------------------------------------------

    # ---- filtering -------------------------------------------------------------------------------

    def test_models_filter(self):
        t = trace_all({'a': "SELECT 1 AS x", 'b': "SELECT 2 AS y"}, models=['a'])
        self.assertEqual(['a'], list(t.tables))

    def test_excluded_models_filter(self):
        t = trace_all({'a': "SELECT 1 AS x", 'b': "SELECT 2 AS y"}, excluded_models=['a'])
        self.assertEqual(['b'], list(t.tables))

    def test_models_takes_precedence_over_excluded_models(self):
        t = trace_all({'a': "SELECT 1 AS x", 'b': "SELECT 2 AS y"}, models=['a'], excluded_models=['a'])
        self.assertEqual(['a'], list(t.tables))

    def test_filtered_out_model_is_not_resolvable(self):
        t = trace_all({'a': "SELECT first_name AS fn FROM db.people", 'b': "SELECT fn FROM a"}, models=['b'])
        self.assertEqual(['b'], list(t.tables))
        self.assert_source(t.table('b'), 'fn', UNKNOWN)

    def test_models_filter_matching_nothing(self):
        t = trace_all({'a': "SELECT 1 AS x"}, models=['zzz'])
        self.assertEqual({}, t.tables)

    # ---- naming ----------------------------------------------------------------------------------

    def test_db_and_catalog_qualify_model_names(self):
        t = trace_all({'x': "SELECT first_name AS fn FROM db.people"}, db='d', catalog='c')
        self.assertEqual(['c.d.x'], list(t.tables))
        table = t.table('c.d.x')
        self.assertEqual('x', table.name)
        self.assertEqual('d', table.db)
        self.assertEqual('c', table.catalog)

    def test_qualified_model_ids_are_parsed(self):
        t = trace_all({'c.d.x': "SELECT first_name AS fn FROM db.people"})
        table = t.table('c.d.x')
        self.assertEqual(('x', 'd', 'c'), (table.name, table.db, table.catalog))

    def test_qualified_model_can_be_referenced_by_full_id(self):
        t = trace_all({
            'c.d.x': "SELECT first_name AS fn FROM db.people",
            'c.d.y': "SELECT fn FROM c.d.x",
        })
        self.assert_source(t.table('c.d.y'), 'fn', col('people', 'first_name'))

    def test_single_statement_requires_a_name(self):
        with self.assertRaises(ValueError):
            SqlTrace.trace_sql("SELECT 1", dialect=DIALECT)

    def test_single_statement_uses_given_name(self):
        t = trace_all("SELECT 1 AS x", 'my_model')
        self.assertEqual(['my_model'], list(t.tables))

    # ---- errors ----------------------------------------------------------------------------------

    def test_invalid_sql_raises_value_error_naming_the_model(self):
        with self.assertRaises(ValueError) as ctx:
            trace_all("SELEC 1", 'broken')
        self.assertIn('broken', str(ctx.exception))

    def test_one_invalid_model_fails_the_whole_trace(self):
        with self.assertRaises(ValueError):
            trace_all({'ok': "SELECT 1 AS x", 'broken': "SELECT FROM WHERE"})

    def test_empty_model_set(self):
        t = trace_all({})
        self.assertEqual({}, t.tables)

    # ---- result API ------------------------------------------------------------------------------

    def test_table_lookup(self):
        t = trace_all({'a': "SELECT 1 AS x"})
        self.assertIs(t.tables['a'], t.table('a'))

    def test_missing_table_lookup_raises_key_error(self):
        t = trace_all({'a': "SELECT 1 AS x"})
        with self.assertRaises(KeyError):
            t.table('nope')

    def test_str_lists_every_table_and_column(self):
        t = trace_all({'a': "SELECT first_name AS fn FROM db.people", 'b': "SELECT 1 AS one"})
        s = str(t)
        self.assertIn('a\n', s)
        self.assertIn('fn:', s)
        self.assertIn('b\n', s)
        self.assertIn('one:', s)

    def test_to_graph(self):
        t = trace_all({'a': "SELECT first_name AS fn FROM db.people"})
        self.assertIsInstance(t.to_graph(), SqlGraph)

    def test_traced_tables_are_serializable(self):
        t = trace_all({
            'a': "SELECT first_name AS fn, age + 1 AS next_age FROM db.people",
            'b': "SELECT fn FROM a",
        })
        for table in t.tables.values():
            d = table.to_dict()
            self.assertEqual(table.name, d['name'])
            self.assertEqual(list(table.columns), d['columns'])
            self.assertEqual(set(table.columns), set(d['sources']))


class FileTraceTests(TraceTestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = self.dir.name
        os.makedirs(os.path.join(self.root, 'sub'))
        self.write('m1.sql', "SELECT first_name AS fn FROM db.people")
        self.write(os.path.join('sub', 'm2.sql'), "SELECT fn FROM m1")

    def write(self, relative_path, sql):
        with open(os.path.join(self.root, relative_path), 'w') as f:
            f.write(sql)

    def test_trace_file_names_model_after_file(self):
        t = SqlTrace.trace_file(os.path.join(self.root, 'm1.sql'), dialect=DIALECT, schema=SCHEMA)
        self.assertEqual(['m1'], list(t.tables))
        self.assert_source(t.table('m1'), 'fn', col('people', 'first_name'))

    def test_trace_file_with_explicit_name(self):
        t = SqlTrace.trace_file(os.path.join(self.root, 'm1.sql'), name='custom', dialect=DIALECT, schema=SCHEMA)
        self.assertEqual(['custom'], list(t.tables))

    def test_trace_directory_walks_subdirectories(self):
        t = SqlTrace.trace_directory(self.root, dialect=DIALECT, schema=SCHEMA)
        self.assertEqual(['m1', 'm2'], sorted(t.tables))
        self.assert_source(t.table('m2'), 'fn', col('people', 'first_name'))

    def test_trace_directory_passes_filters_through(self):
        t = SqlTrace.trace_directory(self.root, dialect=DIALECT, schema=SCHEMA, models=['m1'])
        self.assertEqual(['m1'], list(t.tables))

    def test_list_models(self):
        self.assertEqual(['m1', 'm2'], sorted(SqlTrace.list_models(self.root)))

    def test_list_models_empty_directory(self):
        with tempfile.TemporaryDirectory() as empty:
            self.assertEqual([], SqlTrace.list_models(empty))


if __name__ == '__main__':
    unittest.main()
