"""
Shared helpers for the tracer tests.

The traced model is a graph of Source/TableSource objects that reference each other through intermediate
tables.  `resolve` collapses that graph into plain nested tuples (following ColumnSources through
intermediate tables down to real schema tables) so tests can assert on the *lineage* of a column without
caring about the names of the intermediate tables the tracer generated.
"""
import unittest

from sqlgraph import model as mdl
from sqlgraph.schema import DictSchema
from sqlgraph.trace import SqlTrace

DIALECT = 'postgres'

# cat.db.people / cat.db.depts are the tables used by most tests (always reference them as db.people / db.depts).
# cat.other.people exists so that an unqualified `people` is ambiguous.
SCHEMA = DictSchema({
    'cat': {
        'db': {
            'people': ['id', 'first_name', 'last_name', 'age', 'dept_id', 'data'],
            'depts': ['id', 'name'],
        },
        'other': {
            'people': ['pid', 'nick'],
        },
    }
})


def trace_all(sql, name=None, **kwargs):
    kwargs.setdefault('dialect', DIALECT)
    kwargs.setdefault('schema', SCHEMA)
    return SqlTrace.trace_sql(sql, name, **kwargs)


def trace(sql, name='m', **kwargs):
    """Traces a single statement and returns its TableSource"""
    return trace_all(sql, name, **kwargs).tables[name]


# ---- expected-value builders -------------------------------------------------------------------------

def col(table, column, db='db', catalog='cat'):
    return ('col', f'{catalog}.{db}.{table}.{column}')


def const(value):
    return ('const', value)


UNKNOWN = ('unknown',)


def transform(name, sources):
    return ('transform', name, sources)


def composite(name, sources):
    return ('composite', name, sources)


def union(*sources):
    return ('union', list(sources))


# ---- resolution --------------------------------------------------------------------------------------

def resolve(source, _seen=frozenset()):
    if id(source) in _seen:
        return ('cycle',)
    seen = _seen | {id(source)}

    def r(s):
        return resolve(s, seen)

    def r_children(children):
        if isinstance(children, dict):
            return {k: r(v) for k, v in children.items()}
        return [r(x) for x in (children or [])]

    if isinstance(source, mdl.ColumnSource):
        table = source.table
        if isinstance(table, mdl.TableSource):
            return r(table.sources[source.column])
        table_id = table if isinstance(table, str) else table.id
        return ('col', f'{table_id}.{source.column}')
    if isinstance(source, mdl.ConstantSource):
        return ('const', source.value)
    if isinstance(source, mdl.UnknownSource):
        return UNKNOWN
    if isinstance(source, mdl.PathSource):
        return ('path', source.path, r(source.source))
    if isinstance(source, mdl.UnionSource):
        return ('union', r_children(source.sources))
    if isinstance(source, mdl.ConditionalSource):
        return ('cond', r(source.condition), r(source.true_value), r(source.false_value))
    if isinstance(source, mdl.StructSource):
        return ('struct', r_children(source.sources))
    if isinstance(source, mdl.TransformSource):
        return ('transform', source.name, r_children(source.sources))
    if isinstance(source, mdl.CompositeSource):
        return ('composite', source.name, r_children(source.sources))
    raise AssertionError(f'unexpected source type: {type(source).__name__}')


def leaves(source):
    """
    The set of ultimate inputs to a source: 'db.table.column' strings for real table columns,
    'CONSTANT' for constants and 'UNKNOWN' for unresolved sources.
    """
    found = set()

    def walk(x):
        if isinstance(x, tuple):
            if x[0] == 'col':
                found.add(x[1])
            elif x[0] == 'const':
                found.add('CONSTANT')
            elif x[0] == 'unknown':
                found.add('UNKNOWN')
            else:
                for part in x[1:]:
                    walk(part)
        elif isinstance(x, list):
            for y in x:
                walk(y)
        elif isinstance(x, dict):
            for y in x.values():
                walk(y)

    walk(resolve(source))
    return found


class TraceTestCase(unittest.TestCase):
    maxDiff = None

    def assert_columns(self, ts, expected):
        """Asserts the traced table's columns, in order"""
        self.assertEqual(list(expected), list(ts.columns))

    def assert_source(self, ts, column, expected):
        self.assertIn(column, ts.sources, f'column {column} not found in {list(ts.sources)}')
        self.assertEqual(expected, resolve(ts.sources[column]))

    def assert_sources(self, ts, expected):
        """expected: {column: expected_resolved_source}; also asserts there are no other columns"""
        self.assertEqual(set(expected), set(ts.sources))
        for column, source in expected.items():
            self.assert_source(ts, column, source)
