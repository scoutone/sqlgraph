from test.trace.helpers import TraceTestCase, trace, col, const, UNKNOWN


class SelectTraceTests(TraceTestCase):
    """Column lineage through plain selects, joins, subqueries and CTEs"""

    # ---- basic columns ---------------------------------------------------------------------------

    def test_columns_and_aliases(self):
        ts = trace("SELECT id, first_name AS fn FROM db.people")
        self.assertEqual('table', ts.type)
        self.assert_columns(ts, ['id', 'fn'])
        self.assert_sources(ts, {
            'id': col('people', 'id'),
            'fn': col('people', 'first_name'),
        })

    def test_column_order_is_select_order(self):
        ts = trace("SELECT last_name, id, first_name FROM db.people")
        self.assert_columns(ts, ['last_name', 'id', 'first_name'])

    def test_duplicate_source_column_under_different_aliases(self):
        ts = trace("SELECT age AS a, age AS b FROM db.people")
        self.assert_sources(ts, {'a': col('people', 'age'), 'b': col('people', 'age')})

    def test_table_alias_qualified_columns(self):
        ts = trace("SELECT p.id, p.first_name AS fn FROM db.people p")
        self.assert_sources(ts, {'id': col('people', 'id'), 'fn': col('people', 'first_name')})

    def test_fully_qualified_catalog_table(self):
        ts = trace("SELECT pid, nick FROM cat.other.people")
        self.assert_sources(ts, {
            'pid': col('people', 'pid', db='other'),
            'nick': col('people', 'nick', db='other'),
        })

    def test_same_table_name_in_different_schemas(self):
        ts = trace("SELECT a.id, b.pid FROM db.people a JOIN other.people b ON a.id = b.pid")
        self.assert_sources(ts, {
            'id': col('people', 'id'),
            'pid': col('people', 'pid', db='other'),
        })

    # ---- stars -----------------------------------------------------------------------------------

    def test_star(self):
        ts = trace("SELECT * FROM db.people")
        self.assert_columns(ts, ['id', 'first_name', 'last_name', 'age', 'dept_id', 'data'])
        for c in ts.columns:
            self.assert_source(ts, c, col('people', c))

    def test_qualified_star(self):
        ts = trace("SELECT p.* FROM db.people p")
        self.assert_columns(ts, ['id', 'first_name', 'last_name', 'age', 'dept_id', 'data'])
        self.assert_source(ts, 'age', col('people', 'age'))

    def test_star_mixed_with_columns(self):
        ts = trace("SELECT *, age + 1 AS next_age FROM db.people")
        self.assertIn('next_age', ts.columns)
        self.assertIn('first_name', ts.columns)
        self.assert_source(ts, 'first_name', col('people', 'first_name'))

    def test_star_over_join_includes_both_sides(self):
        ts = trace("SELECT * FROM db.people p JOIN db.depts d ON p.dept_id = d.id")
        self.assertIn('first_name', ts.columns)
        self.assertIn('name', ts.columns)

    def test_star_over_subquery(self):
        ts = trace("SELECT * FROM (SELECT first_name AS fn, age FROM db.people) x")
        self.assert_columns(ts, ['fn', 'age'])
        self.assert_sources(ts, {'fn': col('people', 'first_name'), 'age': col('people', 'age')})

    # ---- joins -----------------------------------------------------------------------------------

    def test_inner_join(self):
        ts = trace("SELECT p.id, d.name AS dn FROM db.people p JOIN db.depts d ON p.dept_id = d.id")
        self.assert_sources(ts, {'id': col('people', 'id'), 'dn': col('depts', 'name')})

    def test_left_join_with_unqualified_columns(self):
        ts = trace("SELECT first_name, name FROM db.people p LEFT JOIN db.depts d ON p.dept_id = d.id")
        self.assert_sources(ts, {
            'first_name': col('people', 'first_name'),
            'name': col('depts', 'name'),
        })

    def test_self_join_same_column_name(self):
        ts = trace("SELECT a.id AS a_id, b.id AS b_id FROM db.people a JOIN db.people b ON a.id = b.dept_id")
        self.assert_sources(ts, {'a_id': col('people', 'id'), 'b_id': col('people', 'id')})

    def test_multiple_joins(self):
        ts = trace("""
            SELECT p.first_name, d.name, q.last_name
            FROM db.people p
            JOIN db.depts d ON d.id = p.dept_id
            JOIN db.people q ON q.id = p.id
        """)
        self.assert_sources(ts, {
            'first_name': col('people', 'first_name'),
            'name': col('depts', 'name'),
            'last_name': col('people', 'last_name'),
        })

    def test_unqualified_column_in_join_condition_resolves(self):
        ts = trace("""
            SELECT s.id, o.age AS other_age
            FROM db.people s
            JOIN db.people o ON o.id = s.dept_id
        """)
        self.assert_sources(ts, {'id': col('people', 'id'), 'other_age': col('people', 'age')})

    def test_comma_join(self):
        ts = trace("SELECT p.id, d.name FROM db.people p, db.depts d WHERE p.dept_id = d.id")
        self.assert_sources(ts, {'id': col('people', 'id'), 'name': col('depts', 'name')})

    # ---- subqueries ------------------------------------------------------------------------------

    def test_subquery_in_from(self):
        ts = trace("SELECT x.fn FROM (SELECT first_name AS fn FROM db.people) x")
        self.assert_source(ts, 'fn', col('people', 'first_name'))

    def test_nested_subqueries(self):
        ts = trace("SELECT y.z FROM (SELECT x.a AS z FROM (SELECT age AS a FROM db.people) x) y")
        self.assert_source(ts, 'z', col('people', 'age'))

    def test_subquery_join(self):
        ts = trace("""
            SELECT p.id, s.name
            FROM db.people p
            JOIN (SELECT id, name FROM db.depts) s ON s.id = p.dept_id
        """)
        self.assert_sources(ts, {'id': col('people', 'id'), 'name': col('depts', 'name')})

    def test_subqueries_in_where_do_not_affect_output(self):
        ts = trace("SELECT id FROM db.people WHERE dept_id IN (SELECT id FROM db.depts)")
        self.assert_sources(ts, {'id': col('people', 'id')})

    def test_correlated_exists_subquery(self):
        ts = trace("""
            SELECT id FROM db.people p
            WHERE EXISTS (SELECT 1 FROM db.depts d WHERE d.id = p.dept_id)
        """)
        self.assert_sources(ts, {'id': col('people', 'id')})

    # ---- CTEs ------------------------------------------------------------------------------------

    def test_cte(self):
        ts = trace("WITH c AS (SELECT first_name AS fn FROM db.people) SELECT fn FROM c")
        self.assert_source(ts, 'fn', col('people', 'first_name'))

    def test_cte_with_star(self):
        ts = trace("WITH c AS (SELECT * FROM db.people) SELECT first_name FROM c")
        self.assert_source(ts, 'first_name', col('people', 'first_name'))

    def test_cte_chain(self):
        ts = trace("""
            WITH a AS (SELECT id FROM db.people),
                 b AS (SELECT id AS i2 FROM a)
            SELECT i2 FROM b
        """)
        self.assert_source(ts, 'i2', col('people', 'id'))

    def test_cte_referenced_twice(self):
        ts = trace("""
            WITH c AS (SELECT id, age FROM db.people)
            SELECT a.id AS aid, b.age AS bage FROM c a JOIN c b ON a.id = b.id
        """)
        self.assert_sources(ts, {'aid': col('people', 'id'), 'bage': col('people', 'age')})

    def test_cte_shadows_real_table_of_same_name(self):
        ts = trace("WITH people AS (SELECT 1 AS id) SELECT id FROM people")
        self.assert_source(ts, 'id', const('1'))

    def test_cte_defined_in_subquery_scope(self):
        ts = trace("""
            SELECT x.v FROM (
              WITH c AS (SELECT age AS v FROM db.people) SELECT v FROM c
            ) x
        """)
        self.assert_source(ts, 'v', col('people', 'age'))

    def test_cte_joined_with_table(self):
        ts = trace("""
            WITH c AS (SELECT id, name FROM db.depts)
            SELECT p.first_name, c.name FROM db.people p JOIN c ON c.id = p.dept_id
        """)
        self.assert_sources(ts, {
            'first_name': col('people', 'first_name'),
            'name': col('depts', 'name'),
        })

    # ---- clauses that must not change lineage ----------------------------------------------------

    def test_group_by_having(self):
        ts = trace("SELECT dept_id FROM db.people GROUP BY dept_id HAVING COUNT(*) > 1")
        self.assert_sources(ts, {'dept_id': col('people', 'dept_id')})

    def test_order_by_limit(self):
        ts = trace("SELECT id FROM db.people ORDER BY age LIMIT 5")
        self.assert_sources(ts, {'id': col('people', 'id')})

    def test_distinct(self):
        ts = trace("SELECT DISTINCT dept_id FROM db.people")
        self.assert_sources(ts, {'dept_id': col('people', 'dept_id')})

    def test_where_clause(self):
        ts = trace("SELECT id FROM db.people WHERE age > 21 AND first_name = 'x'")
        self.assert_sources(ts, {'id': col('people', 'id')})

    # ---- unresolvable references -----------------------------------------------------------------

    def test_unknown_table_yields_unknown_source(self):
        ts = trace("SELECT zz FROM nope")
        self.assert_columns(ts, ['zz'])
        self.assert_source(ts, 'zz', UNKNOWN)

    def test_unknown_column_yields_unknown_source(self):
        ts = trace("SELECT zz FROM db.people")
        self.assert_source(ts, 'zz', UNKNOWN)

    def test_ambiguous_unqualified_table_yields_unknown_source(self):
        # `people` exists in both db and other
        ts = trace("SELECT id FROM people")
        self.assert_source(ts, 'id', UNKNOWN)

    def test_known_columns_still_resolve_next_to_unknown_ones(self):
        ts = trace("SELECT id, zz FROM db.people")
        self.assert_sources(ts, {'id': col('people', 'id'), 'zz': UNKNOWN})
