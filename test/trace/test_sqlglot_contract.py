"""
Contract tests for the sqlglot AST shapes the tracer depends on.

The tracer walks sqlglot's expression tree and reads specific arg names and node types.  sqlglot changes these
between releases (e.g. `Select.args['from']` became `'from_'` in 28.0, `DENSE_RANK()` went from `Anonymous` to a
typed `DenseRank` in 27.11, `IS NOT NULL` went from `Not(Is)` to `Is(negate=True)` in 30.x) and a tracer that no
longer recognises a shape usually degrades quietly to an UNKNOWN source rather than failing.

These tests assert the shapes directly, so after a sqlglot upgrade a failure here names the exact construct that
moved.  Each test accepts every shape the tracer knows how to handle (the old and the new one across the renames
above) and fails on anything else: when bumping the pin, a failure here means a *new* shape that
`sqlgraph/trace.py` must be taught to handle before the expectation is widened.
"""
import unittest

import sqlglot
from sqlglot import exp

from sqlgraph.trace import _arg, _FORMERLY_ANONYMOUS
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
        # trace.py reads it through _arg(select, 'from'): args['from'] before sqlglot 28, args['from_'] after
        s = parse('SELECT 1 FROM t')
        self.assertShape(isinstance(_arg(s, 'from'), exp.From),
                         f"Select FROM is neither args['from'] nor args['from_'] (args: {sorted(s.args)})")

    def test_with_arg_key(self):
        # get_comments reads the WITH clause's comments through _arg(select, 'with'): args['with'] / args['with_']
        s = parse('WITH c AS (SELECT 1 AS a) SELECT * FROM c')
        self.assertShape(isinstance(_arg(s, 'with'), exp.With),
                         f"Select WITH is neither args['with'] nor args['with_'] (args: {sorted(s.args)})")

    def test_from_wraps_its_source_in_this(self):
        s = parse('SELECT 1 FROM t')
        self.assertShape(isinstance(_arg(s, 'from').args.get('this'), exp.Table), 'From.this is not the Table')

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
        t = _arg(parse('SELECT 1 FROM JSON_ARRAY_ELEMENTS(x) WITH ORDINALITY AS a(j, slot)'), 'from').this
        self.assertShape(isinstance(t, exp.Table) and isinstance(t.this, exp.Anonymous),
                         'FROM f() is not Table(Anonymous)')
        self.assertShape(bool(t.args.get('ordinality')), 'WITH ORDINALITY is not Table.args["ordinality"]')
        self.assertShape([c.name for c in t.args['alias'].args['columns']] == ['j', 'slot'], 'Table alias columns changed')

    # ---- expressions the tracer special-cases ----------------------------------------------------

    def test_is_not_null_shape(self):
        # either Not(Is(.., Null)) (older sqlglot) or Is(.., Null, negate=True) (newer): trace.py maps both to NOT(IS NULL)
        e = first_projection('a IS NOT NULL')
        old = isinstance(e, exp.Not) and isinstance(e.this, exp.Is) and isinstance(e.this.expression, exp.Null)
        new = isinstance(e, exp.Is) and isinstance(e.expression, exp.Null) and bool(e.args.get('negate'))
        self.assertShape(old or new, f'IS NOT NULL is neither Not(Is(.., Null)) nor Is(negate=True) (got {e!r})')

    def test_is_null_shape(self):
        e = first_projection('a IS NULL')
        self.assertShape(isinstance(e, exp.Is) and isinstance(e.expression, exp.Null), 'IS NULL is no longer Is(.., Null)')

    def test_dense_rank_shape(self):
        # Window(Anonymous) in older sqlglot, Window(DenseRank) in newer; the latter is in _FORMERLY_ANONYMOUS
        e = first_projection('DENSE_RANK() OVER (ORDER BY a)')
        self.assertShape(isinstance(e, exp.Window), 'DENSE_RANK() OVER is not a Window')
        self.assertShape(isinstance(e.this, exp.Anonymous) or isinstance(e.this, _FORMERLY_ANONYMOUS),
                         f'DENSE_RANK() is {type(e.this).__name__}, which the tracer does not handle')

    def test_promoted_functions_are_handled(self):
        # these were generic Anonymous calls in older sqlglot (traced from their arguments); newer sqlglot types them.
        # A typed node must be listed in trace._FORMERLY_ANONYMOUS or it is traced as UNKNOWN.
        for call in ("REPLACE(a, ' ', '_')", 'ARRAY_REMOVE(a, b)', 'ASCII(a)', 'JSON_OBJECT_AGG(a, b)'):
            e = first_projection(call)
            ok = isinstance(e, exp.Anonymous) or isinstance(e, _FORMERLY_ANONYMOUS) or isinstance(e, exp.JSONObjectAgg)
            self.assertShape(ok, f'{call} parses to {type(e).__name__}: add it to trace._FORMERLY_ANONYMOUS')

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
