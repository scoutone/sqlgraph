"""
Contract tests for the sqlglot AST shapes the tracer depends on.

The tracer walks sqlglot's expression tree and reads specific arg names and node types.  sqlglot changes these
between releases (e.g. `Select.args['from']` became `'from_'` in 28.0, `DENSE_RANK()` went from `Anonymous` to a
typed `DenseRank` in 27.11, `IS NOT NULL` went from `Not(Is)` to `Is(negate=True)` in 30.x) and a tracer that no
longer recognises a shape usually degrades quietly to an UNKNOWN source rather than failing.

These tests assert the shapes directly, so after a sqlglot upgrade a failure here names the exact construct that
moved.  They are written against the version pinned in setup.py: when bumping the pin, update the tracer
(`sqlgraph/trace.py`) to handle the new shape, then update the expectation here.
"""
import unittest

import sqlglot
from sqlglot import exp

from test.trace.helpers import DIALECT


def parse(sql):
    return sqlglot.parse_one(sql, dialect=DIALECT)


def first_projection(sql):
    return parse(f'SELECT {sql} FROM t').expressions[0]


class SqlglotContractTests(unittest.TestCase):

    def assertShape(self, condition, what):
        self.assertTrue(condition, f'sqlglot {sqlglot.__version__}: {what} (see sqlgraph/trace.py)')

    # ---- select structure ------------------------------------------------------------------------

    def test_select_from_arg_key(self):
        # trace.py get_select_sources / union handling read select.args['from']
        s = parse('SELECT 1 FROM t')
        self.assertShape('from' in s.args and isinstance(s.args['from'], exp.From),
                         f"Select FROM is no longer args['from'] (args: {sorted(s.args)})")

    def test_from_wraps_its_source_in_this(self):
        s = parse('SELECT 1 FROM t')
        self.assertShape(isinstance(s.args['from'].args.get('this'), exp.Table), 'From.this is not the Table')

    def test_joins_are_a_list_of_join_nodes(self):
        s = parse('SELECT 1 FROM a JOIN b ON a.x = b.x')
        joins = s.args.get('joins')
        self.assertShape(bool(joins) and isinstance(joins[0], exp.Join), 'Select.args["joins"] is not a list of Join')
        self.assertShape(isinstance(joins[0].args.get('this'), exp.Table), 'Join.this is not the Table')

    def test_cte_and_subquery_aliases(self):
        s = parse('WITH c AS (SELECT 1 AS a) SELECT * FROM c JOIN (SELECT 2 AS b) sq ON TRUE')
        cte = s.find(exp.CTE)
        self.assertShape(cte is not None and cte.alias == 'c' and isinstance(cte.this, exp.Select),
                         'CTE.alias / CTE.this changed')
        sq = s.find(exp.Subquery)
        self.assertShape(sq is not None and sq.alias == 'sq' and isinstance(sq.this, exp.Select),
                         'Subquery.alias / Subquery.this changed')

    def test_union_children(self):
        u = parse('SELECT 1 AS a UNION ALL SELECT 2 AS a')
        self.assertShape(isinstance(u, exp.Union), 'UNION is no longer exp.Union')
        self.assertShape(isinstance(u.this, exp.Select) and isinstance(u.expression, exp.Select),
                         'Union.this / Union.expression are not both Selects')

    # ---- lateral / table functions ---------------------------------------------------------------

    def test_lateral_subquery_shape(self):
        l = parse('SELECT 1 FROM p CROSS JOIN LATERAL (SELECT 1 AS v) AS a(x)').find(exp.Lateral)
        self.assertShape(l is not None and isinstance(l.this, exp.Subquery), 'LATERAL (subquery) is not Lateral(Subquery)')
        alias = l.args.get('alias')
        self.assertShape(alias is not None and [c.name for c in alias.args['columns']] == ['x'],
                         'LATERAL alias column list is not on Lateral.args["alias"].columns')

    def test_lateral_function_shape(self):
        l = parse('SELECT 1 FROM p CROSS JOIN LATERAL JSON_ARRAY_ELEMENTS(p.arr) AS a(j)').find(exp.Lateral)
        self.assertShape(l is not None and isinstance(l.this, exp.Anonymous), 'LATERAL f() is not Lateral(Anonymous)')
        self.assertShape([c.name for c in l.args['alias'].args['columns']] == ['j'], 'LATERAL f() alias columns changed')

    def test_table_function_with_ordinality_shape(self):
        # get_columns_for_table reads table.args['ordinality'] and the alias column list
        t = parse('SELECT 1 FROM JSON_ARRAY_ELEMENTS(x) WITH ORDINALITY AS a(j, slot)').args['from'].this
        self.assertShape(isinstance(t, exp.Table) and isinstance(t.this, exp.Anonymous),
                         'FROM f() is not Table(Anonymous)')
        self.assertShape(bool(t.args.get('ordinality')), 'WITH ORDINALITY is not Table.args["ordinality"]')
        self.assertShape([c.name for c in t.args['alias'].args['columns']] == ['j', 'slot'], 'Table alias columns changed')

    # ---- expressions the tracer special-cases ----------------------------------------------------

    def test_is_not_null_is_not_wrapping_is(self):
        # trace.py maps exp.Is(Null) to 'IS NULL' and exp.Not to 'NOT', which is what produces NOT(IS NULL)
        e = first_projection('a IS NOT NULL')
        self.assertShape(isinstance(e, exp.Not) and isinstance(e.this, exp.Is) and isinstance(e.this.expression, exp.Null),
                         f'IS NOT NULL is no longer Not(Is(.., Null)) (got {e!r})')

    def test_is_null_shape(self):
        e = first_projection('a IS NULL')
        self.assertShape(isinstance(e, exp.Is) and isinstance(e.expression, exp.Null), 'IS NULL is no longer Is(.., Null)')

    def test_dense_rank_is_anonymous(self):
        # trace.py has no DenseRank handling: it relies on the Anonymous path
        e = first_projection('DENSE_RANK() OVER (ORDER BY a)')
        self.assertShape(isinstance(e, exp.Window) and isinstance(e.this, exp.Anonymous),
                         f'DENSE_RANK() is no longer Window(Anonymous) (got {type(e.this).__name__})')

    def test_aggregate_window_shape(self):
        e = first_projection('SUM(a) OVER (PARTITION BY b)')
        self.assertShape(isinstance(e, exp.Window) and isinstance(e.this, exp.Sum), 'SUM() OVER is not Window(Sum)')
        self.assertShape(bool(e.args.get('partition_by')), 'Window.partition_by changed')

    def test_json_arrow_operators(self):
        e = first_projection("j->>'id'")
        self.assertShape(isinstance(e, exp.JSONExtractScalar), "->> is no longer exp.JSONExtractScalar")

    def test_cast_shape(self):
        e = first_projection('a::bigint')
        self.assertShape(isinstance(e, exp.Cast) and isinstance(e.this, exp.Column), 'a::type is not Cast(Column)')

    def test_case_shape(self):
        e = first_projection("CASE WHEN a = 1 THEN 'x' ELSE 'y' END")
        self.assertShape(isinstance(e, exp.Case) and e.args.get('ifs') and e.args.get('default') is not None,
                         'CASE no longer uses args ifs/default')

    def test_json_array_agg_node_type(self):
        # the tracer lists exp.JSONArrayAgg among the aggregates it passes through
        self.assertShape(hasattr(exp, 'JSONArrayAgg') and hasattr(exp, 'ArrayAgg'), 'exp.JSONArrayAgg / exp.ArrayAgg were removed')

    def test_unqualified_column_parent_select(self):
        # the tracer resolves columns through Column.parent_select
        c = parse('SELECT a FROM t').find(exp.Column)
        self.assertShape(isinstance(c.parent_select, exp.Select), 'Column.parent_select no longer returns the Select')


if __name__ == '__main__':
    unittest.main()
