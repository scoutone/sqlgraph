import unittest

import sqlglot
from sqlglot import exp

from sqlgraph import model as mdl
from sqlgraph.trace import SqlTrace
from test.trace.helpers import DIALECT, SCHEMA


def parse(sql):
    return sqlglot.parse_one(sql, dialect=DIALECT)


def selects(tree):
    return list(tree.find_all(exp.Select))


def column(tree, name):
    return next(c for c in tree.find_all(exp.Column) if c.name == name)


class TracerInternalsTests(unittest.TestCase):

    def setUp(self):
        self.tracer = SqlTrace.Tracer({}, dialect=DIALECT, schema=SCHEMA)

    # ---- get_select_sources ----------------------------------------------------------------------

    def test_sources_are_keyed_by_alias_or_name(self):
        q = parse("SELECT 1 FROM db.people p JOIN db.depts ON TRUE")
        self.assertEqual(['p', 'depts'], list(self.tracer.get_select_sources(q)))

    def test_sources_include_from_and_joins_in_order(self):
        q = parse("SELECT 1 FROM a, b JOIN c ON TRUE")
        sources = self.tracer.get_select_sources(q)
        self.assertEqual(['a', 'b', 'c'], list(sources))
        self.assertIsInstance(sources['a'], exp.From)
        self.assertIsInstance(sources['b'], exp.Join)

    def test_unaliased_subquery_source_gets_positional_name(self):
        q = parse("SELECT 1 FROM (SELECT 2)")
        self.assertEqual(['src_0'], list(self.tracer.get_select_sources(q)))

    def test_cte_reference_is_replaced_by_the_cte(self):
        q = parse("WITH c AS (SELECT 1 AS a) SELECT 1 FROM c")
        sources = self.tracer.get_select_sources(q)
        self.assertIsInstance(sources['c'], exp.CTE)

    def test_schema_qualified_table_is_not_replaced_by_same_named_cte(self):
        q = parse("WITH people AS (SELECT 1 AS id) SELECT 1 FROM db.people")
        sources = self.tracer.get_select_sources(q)
        self.assertIsInstance(sources['people'], exp.From)

    def test_select_without_sources_raises(self):
        with self.assertRaises(ValueError):
            self.tracer.get_select_sources(parse("SELECT 1"))

    def test_select_without_sources_in_plain_subquery_raises(self):
        inner = selects(parse("SELECT * FROM (SELECT 1) t"))[1]
        with self.assertRaises(ValueError):
            self.tracer.get_select_sources(inner)

    # ---- get_applicable_ctes ---------------------------------------------------------------------

    def test_ctes_visible_from_top_level_and_nested_selects(self):
        q = parse("WITH c AS (SELECT 1 AS a) SELECT 1 FROM db.people WHERE id IN (SELECT id FROM c)")
        inner = selects(q)[-1]
        self.assertEqual(['c'], [c.alias for c in self.tracer.get_applicable_ctes(q)])
        self.assertEqual(['c'], [c.alias for c in self.tracer.get_applicable_ctes(inner)])

    def test_no_ctes(self):
        self.assertEqual([], self.tracer.get_applicable_ctes(parse("SELECT 1 FROM db.people")))

    def test_ctes_from_every_enclosing_scope_are_visible(self):
        q = parse("""
            WITH outer_cte AS (SELECT 1 AS a)
            SELECT 1 FROM (WITH inner_cte AS (SELECT 2 AS b) SELECT 1 FROM inner_cte) s
        """)
        innermost = selects(q)[-1]
        names = [c.alias for c in self.tracer.get_applicable_ctes(innermost)]
        self.assertIn('outer_cte', names)

    # ---- laterals --------------------------------------------------------------------------------

    def test_enclosing_lateral_of_top_level_select_is_none(self):
        self.assertIsNone(self.tracer.get_enclosing_lateral(parse("SELECT 1 FROM p")))

    def test_enclosing_lateral_of_plain_subquery_is_none(self):
        inner = selects(parse("SELECT * FROM (SELECT 1) t"))[1]
        self.assertIsNone(self.tracer.get_enclosing_lateral(inner))

    def test_enclosing_lateral_of_lateral_body(self):
        inner = selects(parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 AS v) l"))[1]
        self.assertIsInstance(self.tracer.get_enclosing_lateral(inner), exp.Lateral)

    def test_enclosing_lateral_of_nested_derived_table(self):
        inner = selects(parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT * FROM (SELECT 1 AS v) t) l"))[2]
        self.assertIsInstance(self.tracer.get_enclosing_lateral(inner), exp.Lateral)

    def test_enclosing_lateral_of_the_select_that_contains_the_lateral_is_none(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 AS v) l")
        self.assertIsNone(self.tracer.get_enclosing_lateral(selects(q)[0]))

    def test_enclosing_lateral_is_the_nearest_one(self):
        q = parse("""
            SELECT 1 FROM p
            CROSS JOIN LATERAL (
              SELECT 1 FROM q CROSS JOIN LATERAL (SELECT 1 AS v) inner_l
            ) outer_l
        """)
        innermost = selects(q)[-1]
        lateral = self.tracer.get_enclosing_lateral(innermost)
        self.assertEqual('inner_l', lateral.alias)

    def test_lateral_body_sees_sources_to_the_left(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 FROM q) l")
        inner = selects(q)[1]
        self.assertEqual(['q', 'p'], list(self.tracer.get_select_sources(inner)))

    def test_lateral_body_does_not_see_its_own_join_or_later_ones(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 FROM q) l CROSS JOIN r")
        inner = selects(q)[1]
        sources = self.tracer.get_select_sources(inner)
        self.assertNotIn('l', sources)
        self.assertNotIn('r', sources)

    def test_lateral_body_sees_earlier_laterals(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 AS x) a CROSS JOIN LATERAL (SELECT 1 FROM q) b")
        second = selects(q)[2]
        self.assertEqual(['q', 'p', 'a'], list(self.tracer.get_select_sources(second)))

    def test_nested_lateral_sees_sources_of_every_enclosing_lateral(self):
        q = parse("""
            SELECT 1 FROM p
            CROSS JOIN LATERAL (
              SELECT 1 FROM q CROSS JOIN LATERAL (SELECT 1 FROM r) inner_l
            ) outer_l
        """)
        innermost = selects(q)[-1]
        sources = self.tracer.get_select_sources(innermost)
        self.assertEqual({'r', 'q', 'p'}, set(sources))

    def test_own_sources_come_first_and_shadow_outer_sources(self):
        q = parse("SELECT 1 FROM x CROSS JOIN LATERAL (SELECT 1 FROM db.depts x) l")
        inner = selects(q)[1]
        sources = self.tracer.get_select_sources(inner)
        self.assertEqual(['x'], list(sources))
        self.assertIsInstance(sources['x'], exp.From)
        self.assertEqual('depts', sources['x'].this.name)

    def test_include_outer_false_returns_only_own_sources(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 FROM q) l")
        inner = selects(q)[1]
        self.assertEqual(['q'], list(self.tracer.get_select_sources(inner, include_outer=False)))

    def test_include_outer_false_without_own_sources_raises(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 AS v) l")
        inner = selects(q)[1]
        with self.assertRaises(ValueError):
            self.tracer.get_select_sources(inner, include_outer=False)

    def test_outer_cte_sources_are_substituted(self):
        q = parse("WITH c AS (SELECT 1 AS a) SELECT 1 FROM c CROSS JOIN LATERAL (SELECT 1 FROM q) l")
        inner = selects(q)[-1]
        sources = self.tracer.get_select_sources(inner)
        self.assertIsInstance(sources['c'], exp.CTE)

    # ---- get_visible_sources ---------------------------------------------------------------------

    def test_column_in_function_argument_cannot_see_the_function_source(self):
        q = parse("SELECT 1 FROM p CROSS JOIN LATERAL (SELECT * FROM JSON_ARRAY_ELEMENTS(JSON_BUILD_ARRAY(foo))) l")
        visible = self.tracer.get_visible_sources(column(q, 'foo'))
        self.assertEqual(['p'], list(visible))

    def test_column_in_select_list_sees_all_sources(self):
        q = parse("SELECT foo FROM a JOIN b ON TRUE")
        self.assertEqual(['a', 'b'], list(self.tracer.get_visible_sources(column(q, 'foo'))))

    def test_column_in_join_condition_sees_the_joined_source(self):
        q = parse("SELECT 1 FROM a JOIN b ON b.k = foo")
        self.assertEqual(['a', 'b'], list(self.tracer.get_visible_sources(column(q, 'foo'))))

    def test_column_in_where_sees_all_sources(self):
        q = parse("SELECT 1 FROM a, b WHERE foo = 1")
        self.assertEqual(['a', 'b'], list(self.tracer.get_visible_sources(column(q, 'foo'))))

    def test_column_in_cte_body_can_still_see_that_cte(self):
        # a recursive CTE refers to itself, so CTE sources are never hidden
        q = parse("WITH RECURSIVE r AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM r) SELECT id FROM r")
        recursive_branch = next(s for s in selects(q) if s.find(exp.Add))
        visible = self.tracer.get_visible_sources(column(recursive_branch, 'id'))
        self.assertIn('r', visible)
        self.assertIsInstance(visible['r'], exp.CTE)

    def test_column_in_join_function_argument_cannot_see_that_join(self):
        q = parse("SELECT 1 FROM p, UNNEST(ARRAY[foo]) AS u")
        visible = self.tracer.get_visible_sources(column(q, 'foo'))
        self.assertEqual(['p'], list(visible))


class ModelClassTests(unittest.TestCase):
    """Model behavior the tracer depends on"""

    def test_table_ids(self):
        self.assertEqual('t', mdl.Table.get_id('t'))
        self.assertEqual('d.t', mdl.Table.get_id('t', 'd'))
        self.assertEqual('c.d.t', mdl.Table.get_id('t', 'd', 'c'))

    def test_table_id_ignores_catalog_without_db(self):
        self.assertEqual('t', mdl.Table.get_id('t', None, 'c'))

    def test_table_from_id(self):
        t = mdl.Table.from_id('c.d.t')
        self.assertEqual(('t', 'd', 'c'), (t.name, t.db, t.catalog))
        t = mdl.Table.from_id('d.t')
        self.assertEqual(('t', 'd', None), (t.name, t.db, t.catalog))
        t = mdl.Table.from_id('t')
        self.assertEqual(('t', None, None), (t.name, t.db, t.catalog))

    def test_table_matching_ignores_missing_qualifiers(self):
        self.assertTrue(mdl.Table('t', []).matches(mdl.Table('t', [], db='d')))
        self.assertTrue(mdl.Table('t', [], db='d').matches(mdl.Table('t', [])))
        self.assertFalse(mdl.Table('t', [], db='d').matches(mdl.Table('t', [], db='e')))
        self.assertFalse(mdl.Table('t', []).matches(mdl.Table('u', [])))

    def test_table_source_exposes_columns_from_sources(self):
        ts = mdl.TableSource('x', {'a': mdl.ConstantSource('1'), 'b': mdl.ConstantSource('2')}, type='select')
        self.assertEqual(['a', 'b'], list(ts.columns))

    def test_union_source_flattens_nested_unions(self):
        a, b, c = (mdl.ConstantSource(v) for v in 'abc')
        nested = mdl.UnionSource(left=mdl.UnionSource(left=a, right=b), right=c)
        self.assertEqual([a, b, c], nested.sources)

    def test_union_source_does_not_share_state_between_instances(self):
        first = mdl.UnionSource(left=mdl.ConstantSource('1'))
        second = mdl.UnionSource(left=mdl.ConstantSource('2'))
        self.assertEqual(1, len(first.sources))
        self.assertEqual(1, len(second.sources))

    def test_conditional_source_defaults_false_value_to_null(self):
        c = mdl.ConditionalSource(mdl.ConstantSource('c'), mdl.ConstantSource('t'))
        self.assertEqual('NULL', c.false_value.value)

    def test_transform_source_wraps_single_source_in_list(self):
        inner = mdl.ConstantSource('x')
        t = mdl.TransformSource('F', inner)
        self.assertEqual([inner], t.as_list())

    def test_all_sources_flattens_composites(self):
        a, b = mdl.ConstantSource('a'), mdl.ConstantSource('b')
        t = mdl.TransformSource('F', [a, mdl.TransformSource('G', [b])])
        self.assertEqual([a, b], t.all_sources())


class SafeSqlTests(unittest.TestCase):

    def test_renders_a_node_normally(self):
        from sqlgraph.trace import _safe_sql
        self.assertEqual('a + 1', _safe_sql(parse('SELECT a + 1 FROM t').expressions[0]))

    def test_falls_back_when_sqlglot_cannot_render_the_node(self):
        # sqlglot 28.10 - 30.10 raise while generating CHR(..): that must not fail the trace
        from sqlgraph.trace import _safe_sql

        class Unrenderable(exp.Expression):
            def __str__(self):
                raise TypeError("object of type 'Literal' has no len()")

        self.assertEqual('<Unrenderable>', _safe_sql(Unrenderable()))


if __name__ == '__main__':
    unittest.main()
