from test.trace.helpers import (
    TraceTestCase, trace, col, const, transform, union, UNKNOWN, leaves, resolve
)

AGE = col('people', 'age')
ID = col('people', 'id')
FN = col('people', 'first_name')
LN = col('people', 'last_name')
DATA = col('people', 'data')


class LateralTraceTests(TraceTestCase):
    """LATERAL joins: correlated references to sources on the left, alias column lists and table functions"""

    # ---- lateral subqueries with their own FROM ---------------------------------------------------

    def test_lateral_subquery_with_from(self):
        ts = trace("""
            SELECT l.v FROM db.people p
            CROSS JOIN LATERAL (SELECT d.name AS v FROM db.depts d WHERE d.id = p.dept_id) l
        """)
        self.assert_source(ts, 'v', col('depts', 'name'))

    def test_lateral_with_from_correlated_function_arguments(self):
        # the lateral's own FROM is a table function whose arguments reference the outer table
        ts = trace("""
            SELECT a.j, a.slot FROM db.people
            CROSS JOIN LATERAL (
              SELECT * FROM JSON_ARRAY_ELEMENTS(JSON_BUILD_ARRAY(JSON_BUILD_OBJECT('id', id, 'x', first_name = 'Y'))) WITH ORDINALITY
            ) AS a(j, slot)
        """)
        self.assert_columns(ts, ['j', 'slot'])
        self.assert_source(ts, 'j', transform('JSON_ARRAY_ELEMENTS', [
            transform('JSON_BUILD_ARRAY', [
                transform('JSON_BUILD_OBJECT', [
                    const("'id'"), ID,
                    const("'x'"), transform('EQ', {'left': FN, 'right': const("'Y'")}),
                ])
            ])
        ]))
        self.assert_source(ts, 'slot', const('ORDINALITY'))

    def test_lateral_with_from_referencing_aliased_outer_subquery(self):
        # outer source is an aliased subquery referenced with a qualified name
        ts = trace("""
            SELECT a.j FROM (SELECT JSON_BUILD_ARRAY(id, first_name) AS arr FROM db.people) r
            CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(r.arr)) AS a(j)
        """)
        self.assertEqual({'cat.db.people.id', 'cat.db.people.first_name'}, leaves(ts.sources['j']))

    def test_lateral_own_source_shadows_outer_source(self):
        # `age` exists in both the outer table and the lateral's own subquery: the lateral's own wins
        ts = trace("""
            SELECT l.age FROM db.people
            CROSS JOIN LATERAL (SELECT 1 AS age FROM (SELECT 1) t) l
        """)
        self.assert_source(ts, 'age', const('1'))

    def test_lateral_star_does_not_include_outer_columns(self):
        # SELECT * inside the lateral expands only the lateral's own sources
        ts = trace("""
            SELECT a.v FROM db.people
            CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(JSON_BUILD_ARRAY(id))) AS a(v)
        """)
        self.assert_columns(ts, ['v'])

    def test_lateral_star_over_subquery_does_not_include_outer_columns(self):
        ts = trace("""
            SELECT * FROM db.depts d
            CROSS JOIN LATERAL (SELECT * FROM (SELECT d.name AS n) t) l
        """)
        self.assertEqual(['id', 'name', 'n'], list(ts.columns))

    def test_unqualified_function_argument_does_not_recurse(self):
        # regression: an unqualified column inside the FROM function's arguments used to re-trace the function forever
        ts = trace("""
            WITH pivoted AS (
              SELECT (j->>'id')::bigint AS rid, slot
              FROM (
                SELECT a.j, a.slot FROM db.people
                CROSS JOIN LATERAL (
                  SELECT * FROM JSON_ARRAY_ELEMENTS(JSON_BUILD_ARRAY(JSON_BUILD_OBJECT('id', id, 'owner', dept_id))) WITH ORDINALITY
                ) AS a(j, slot)
                WHERE age > 1
              ) s
            )
            SELECT p.rid, p.slot FROM pivoted p
        """)
        self.assert_columns(ts, ['rid', 'slot'])
        # j->>'id' doesn't narrow the JSON object down to one input, so every field of the object contributes
        self.assertEqual({'cat.db.people.id', 'cat.db.people.dept_id', 'CONSTANT'}, leaves(ts.sources['rid']))
        self.assertEqual({'CONSTANT'}, leaves(ts.sources['slot']))

    # ---- lateral subqueries without a FROM --------------------------------------------------------

    def test_lateral_without_from_qualified_outer_reference(self):
        ts = trace("SELECT l.y, l.z FROM db.people p CROSS JOIN LATERAL (SELECT p.age + 1 AS y, 2 AS z) l")
        self.assert_sources(ts, {
            'y': transform('ADD', {'left': AGE, 'right': const('1')}),
            'z': const('2'),
        })

    def test_lateral_without_from_unqualified_outer_reference(self):
        ts = trace("SELECT l.v FROM db.people p CROSS JOIN LATERAL (SELECT first_name AS v) l")
        self.assert_source(ts, 'v', FN)

    def test_comma_lateral(self):
        ts = trace("SELECT l.v FROM db.people p, LATERAL (SELECT p.age AS v) l")
        self.assert_source(ts, 'v', AGE)

    def test_left_join_lateral(self):
        ts = trace("SELECT l.v FROM db.people p LEFT JOIN LATERAL (SELECT p.age AS v) l ON TRUE")
        self.assert_source(ts, 'v', AGE)

    def test_lateral_referencing_cte_outer_source(self):
        ts = trace("""
            WITH c AS (SELECT age FROM db.people)
            SELECT l.v FROM c CROSS JOIN LATERAL (SELECT c.age AS v) l
        """)
        self.assert_source(ts, 'v', AGE)

    def test_lateral_referencing_subquery_outer_source(self):
        ts = trace("""
            SELECT l.v FROM (SELECT age AS a FROM db.people) s
            CROSS JOIN LATERAL (SELECT s.a AS v) l
        """)
        self.assert_source(ts, 'v', AGE)

    def test_lateral_union(self):
        ts = trace("""
            SELECT l.v FROM db.people p
            CROSS JOIN LATERAL (SELECT p.age AS v UNION ALL SELECT p.id AS v) l
        """)
        self.assert_source(ts, 'v', union(AGE, ID))

    # ---- multiple and chained laterals ------------------------------------------------------------

    def test_second_lateral_references_first(self):
        ts = trace("""
            SELECT a.x, b.y FROM db.people p
            CROSS JOIN LATERAL (SELECT p.age AS x) a
            CROSS JOIN LATERAL (SELECT a.x + 1 AS y) b
        """)
        self.assert_sources(ts, {
            'x': AGE,
            'y': transform('ADD', {'left': AGE, 'right': const('1')}),
        })

    def test_lateral_cannot_see_sources_to_its_right(self):
        # `late` is defined after the first lateral, so the first lateral's `age` can only come from p
        ts = trace("""
            SELECT a.x FROM db.people p
            CROSS JOIN LATERAL (SELECT age AS x) a
            CROSS JOIN LATERAL (SELECT 1 AS age) late
        """)
        self.assert_source(ts, 'x', AGE)

    def test_nested_lateral(self):
        ts = trace("""
            SELECT o.v FROM db.people p
            CROSS JOIN LATERAL (
              SELECT i.w AS v FROM (SELECT p.age AS w) i
            ) o
        """)
        self.assert_source(ts, 'v', AGE)

    def test_lateral_nested_in_lateral_can_reference_outermost_source(self):
        ts = trace("""
            SELECT o.v FROM db.people p
            CROSS JOIN LATERAL (
              SELECT i.w AS v FROM db.depts d
              CROSS JOIN LATERAL (SELECT p.age + d.id AS w) i
            ) o
        """)
        self.assertEqual({'cat.db.people.age', 'cat.db.depts.id'}, leaves(ts.sources['v']))

    def test_lateral_inside_cte(self):
        ts = trace("""
            WITH c AS (
              SELECT l.v FROM db.people p CROSS JOIN LATERAL (SELECT p.age AS v) l
            )
            SELECT v FROM c
        """)
        self.assert_source(ts, 'v', AGE)

    def test_lateral_inside_subquery(self):
        ts = trace("""
            SELECT s.v FROM (
              SELECT l.v FROM db.people p CROSS JOIN LATERAL (SELECT p.age AS v) l
            ) s
        """)
        self.assert_source(ts, 'v', AGE)

    # ---- alias column lists -----------------------------------------------------------------------

    def test_alias_columns_rename_by_position(self):
        ts = trace("""
            SELECT a.first_col, a.second_col FROM db.people p
            CROSS JOIN LATERAL (SELECT p.id AS x, p.first_name AS y) AS a(first_col, second_col)
        """)
        self.assert_columns(ts, ['first_col', 'second_col'])
        self.assert_sources(ts, {'first_col': ID, 'second_col': FN})

    def test_alias_columns_rename_ignores_original_names(self):
        ts = trace("""
            SELECT a.y, a.x FROM db.people p
            CROSS JOIN LATERAL (SELECT p.id AS x, p.first_name AS y) AS a(y, x)
        """)
        # the renaming is positional: the *first* output column is now called y
        self.assert_sources(ts, {'y': ID, 'x': FN})

    def test_alias_with_fewer_columns_keeps_remaining_names(self):
        ts = trace("""
            SELECT a.x, a.id FROM db.people p
            CROSS JOIN LATERAL (SELECT p.age, p.id) AS a(x)
        """)
        self.assert_sources(ts, {'x': AGE, 'id': ID})

    def test_alias_without_column_list_keeps_names(self):
        ts = trace("SELECT l.age FROM db.people p CROSS JOIN LATERAL (SELECT p.age) l")
        self.assert_source(ts, 'age', AGE)

    def test_alias_columns_over_star_subquery(self):
        ts = trace("""
            SELECT a.n FROM db.depts d
            CROSS JOIN LATERAL (SELECT * FROM (SELECT d.name) t) AS a(n)
        """)
        self.assert_source(ts, 'n', col('depts', 'name'))

    # ---- table functions --------------------------------------------------------------------------

    def test_lateral_unnest(self):
        ts = trace("SELECT u FROM db.people p CROSS JOIN LATERAL UNNEST(ARRAY[p.first_name, p.last_name]) AS u")
        self.assert_source(ts, 'u', transform('UNNEST', [transform('ARRAY', [FN, LN])]))

    def test_table_function_ordinality_is_constant_and_value_is_traced(self):
        ts = trace("""
            SELECT a.j, a.slot FROM db.people
            CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(JSON_BUILD_ARRAY(id)) WITH ORDINALITY) AS a(j, slot)
        """)
        self.assert_source(ts, 'slot', const('ORDINALITY'))
        self.assert_source(ts, 'j', transform('JSON_ARRAY_ELEMENTS', [transform('JSON_BUILD_ARRAY', [ID])]))

    def test_table_function_with_own_alias_column_list(self):
        ts = trace("""
            SELECT t.v, t.k FROM db.people p
            CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(p.data) WITH ORDINALITY AS e(v, k)) t
        """)
        self.assert_source(ts, 'v', transform('JSON_ARRAY_ELEMENTS', [DATA]))
        self.assert_source(ts, 'k', const('ORDINALITY'))

    def test_table_function_without_alias_names_column_after_function(self):
        # Postgres exposes the function's output under the function's name when there is no alias
        ts = trace("SELECT json_array_elements FROM JSON_ARRAY_ELEMENTS('[1]'::json)")
        self.assert_source(ts, 'json_array_elements', transform('JSON_ARRAY_ELEMENTS', [const("'[1]'")]))

    def test_table_function_without_alias_with_ordinality(self):
        ts = trace("SELECT json_array_elements, ordinality FROM JSON_ARRAY_ELEMENTS('[1]'::json) WITH ORDINALITY")
        self.assert_source(ts, 'ordinality', const('ORDINALITY'))
        self.assert_source(ts, 'json_array_elements', transform('JSON_ARRAY_ELEMENTS', [const("'[1]'")]))

    def test_plain_table_function_with_alias_column_list(self):
        ts = trace("SELECT e.v FROM JSON_ARRAY_ELEMENTS('[1]'::json) AS e(v)")
        self.assert_source(ts, 'v', transform('JSON_ARRAY_ELEMENTS', [const("'[1]'")]))

    def test_plain_table_function_ordinality_alias(self):
        ts = trace("SELECT e.v, e.n FROM JSON_ARRAY_ELEMENTS('[1]'::json) WITH ORDINALITY AS e(v, n)")
        self.assert_source(ts, 'n', const('ORDINALITY'))
        self.assert_source(ts, 'v', transform('JSON_ARRAY_ELEMENTS', [const("'[1]'")]))

    # ---- end to end shape of the original problem -------------------------------------------------

    def test_pivot_through_lateral_json_array(self):
        """The pattern used by the family history / review of systems models"""
        ts = trace("""
            WITH pivoted AS (
              SELECT
                (j->>'person_id')::bigint AS person_id,
                (j->>'label') AS label,
                slot
              FROM (
                SELECT a.j, a.slot
                FROM (
                  SELECT JSON_BUILD_ARRAY(
                    JSON_BUILD_OBJECT('person_id', id, 'label', 'first',  'value', first_name),
                    JSON_BUILD_OBJECT('person_id', id, 'label', 'second', 'value', last_name)
                  ) AS row_array
                  FROM db.people
                  WHERE age > 0
                ) r
                CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(r.row_array) WITH ORDINALITY) AS a(j, slot)
              ) s
            )
            SELECT p.person_id, p.label, p.slot, d.name
            FROM pivoted p JOIN db.depts d ON d.id = p.person_id
        """)
        self.assert_columns(ts, ['person_id', 'label', 'slot', 'name'])
        self.assertEqual(
            ('path', 'person_id'),
            resolve(ts.sources['person_id'])[:2]
        )
        self.assertIn('cat.db.people.id', leaves(ts.sources['person_id']))
        self.assertIn('cat.db.people.first_name', leaves(ts.sources['label']))
        self.assertNotIn('UNKNOWN', leaves(ts.sources['person_id']))
        self.assertEqual({'CONSTANT'}, leaves(ts.sources['slot']))
        self.assert_source(ts, 'name', col('depts', 'name'))
