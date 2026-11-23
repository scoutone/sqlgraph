import unittest

from test.trace.helpers import TraceTestCase, trace, col, const, union, resolve, leaves

PEOPLE_ID = col('people', 'id')
DEPTS_ID = col('depts', 'id')


class UnionTraceTests(TraceTestCase):

    def test_union_all(self):
        ts = trace("SELECT id AS v FROM db.people UNION ALL SELECT id AS v FROM db.depts")
        self.assertEqual('union', ts.type)
        self.assert_columns(ts, ['v'])
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID))

    def test_union_distinct(self):
        ts = trace("SELECT id AS v FROM db.people UNION SELECT id AS v FROM db.depts")
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID))

    def test_three_way_union_is_flat(self):
        ts = trace("""
            SELECT id AS v FROM db.people
            UNION ALL SELECT id AS v FROM db.depts
            UNION ALL SELECT age AS v FROM db.people
        """)
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID, col('people', 'age')))

    def test_union_branches_keep_order(self):
        ts = trace("SELECT id AS v FROM db.depts UNION ALL SELECT id AS v FROM db.people")
        self.assert_source(ts, 'v', union(DEPTS_ID, PEOPLE_ID))

    def test_union_multiple_columns(self):
        ts = trace("""
            SELECT id AS a, first_name AS b FROM db.people
            UNION ALL
            SELECT id AS a, name AS b FROM db.depts
        """)
        self.assert_columns(ts, ['a', 'b'])
        self.assert_source(ts, 'a', union(PEOPLE_ID, DEPTS_ID))
        self.assert_source(ts, 'b', union(col('people', 'first_name'), col('depts', 'name')))

    def test_union_with_constants(self):
        ts = trace("SELECT id AS v FROM db.people UNION ALL SELECT 0 AS v")
        self.assert_source(ts, 'v', union(PEOPLE_ID, const('0')))

    def test_union_of_expressions(self):
        ts = trace("SELECT age + 1 AS v FROM db.people UNION ALL SELECT id AS v FROM db.depts")
        self.assertEqual({'cat.db.people.age', 'CONSTANT', 'cat.db.depts.id'}, leaves(ts.sources['v']))

    def test_union_inside_subquery(self):
        ts = trace("SELECT s.v FROM (SELECT id AS v FROM db.people UNION ALL SELECT id AS v FROM db.depts) s")
        self.assertEqual('table', ts.type)
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID))

    def test_union_of_ctes(self):
        ts = trace("""
            WITH a AS (SELECT id AS v FROM db.people),
                 b AS (SELECT id AS v FROM db.depts)
            SELECT v FROM a UNION ALL SELECT v FROM b
        """)
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID))

    def test_union_in_cte(self):
        ts = trace("""
            WITH u AS (SELECT id AS v FROM db.people UNION ALL SELECT id AS v FROM db.depts)
            SELECT v AS w FROM u
        """)
        self.assert_source(ts, 'w', union(PEOPLE_ID, DEPTS_ID))

    def test_union_of_joins(self):
        ts = trace("""
            SELECT p.id AS v FROM db.people p JOIN db.depts d ON d.id = p.dept_id
            UNION ALL
            SELECT d.id AS v FROM db.depts d
        """)
        self.assert_source(ts, 'v', union(PEOPLE_ID, DEPTS_ID))

    @unittest.expectedFailure
    def test_union_matches_columns_by_position_not_name(self):
        # In SQL the output names come from the FIRST branch and later branches line up by position.  The tracer
        # currently looks later branches up by the first branch's names, so this raises a KeyError.
        ts = trace("SELECT id AS v FROM db.people UNION ALL SELECT name FROM db.depts")
        self.assert_source(ts, 'v', union(PEOPLE_ID, col('depts', 'name')))


class RecursiveCteTraceTests(TraceTestCase):

    def test_recursive_cte_traces_anchor_columns(self):
        ts = trace("""
            WITH RECURSIVE r AS (
              SELECT id FROM db.people
              UNION ALL
              SELECT r.id + 1 FROM r WHERE r.id < 5
            )
            SELECT id FROM r
        """)
        self.assert_columns(ts, ['id'])
        self.assertIn('cat.db.people.id', leaves(ts.sources['id']))
        self.assertNotIn('UNKNOWN', leaves(ts.sources['id']))

    def test_recursive_cte_does_not_loop_or_self_reference(self):
        ts = trace("""
            WITH RECURSIVE tree AS (
              SELECT id, dept_id FROM db.people WHERE dept_id IS NULL
              UNION ALL
              SELECT p.id, p.dept_id FROM db.people p JOIN tree t ON p.dept_id = t.id
            )
            SELECT id, dept_id FROM tree
        """)
        self.assert_columns(ts, ['id', 'dept_id'])
        self.assertEqual({'cat.db.people.id'}, leaves(ts.sources['id']))
        self.assertEqual({'cat.db.people.dept_id'}, leaves(ts.sources['dept_id']))

    def test_recursive_cte_with_extra_computed_column(self):
        ts = trace("""
            WITH RECURSIVE r AS (
              SELECT id, 1 AS depth FROM db.people
              UNION ALL
              SELECT r.id, r.depth + 1 FROM r WHERE r.depth < 3
            )
            SELECT id, depth FROM r
        """)
        self.assertIn('CONSTANT', leaves(ts.sources['depth']))
        self.assertNotIn('UNKNOWN', leaves(ts.sources['depth']))
