import unittest
from sqlgraph.schema import DictSchema
from sqlgraph.trace import SqlTrace
from test.dialect import PostgresExtended
from sqlgraph.filter import SimpleFilter
import json
from sqlgraph.transform import MappingTransform


TABLES = {
    'test_db': {
        'public': {
            'default_table_a': [
                'field_1',
                'field_2'
            ]
        },
        'schema_1': {
            'source_table_b': [
                'field_b1',
                'field_b2'
            ],
            'source_table_c': [
                'field_c1',
                'field_c2'
            ],
            'intermediate_table_c': [
                'constant_field',
                'c1',
                'c2'
            ]  ,
            'dest_table_b': [
                'constant_field',
                'b1',
                'b2'
            ]
        }
    }
}

SQLs = {
    'dest_table_a': """\
        SELECT
          *
        FROM public.default_table_a
        LIMIT 0
    """,
    'dest_table_b': """\
        SELECT
            NULL::TEXT as constant_field,
            field_b1 AS b1,
            field_b2 AS b2
        FROM schema_1.source_table_b
    """,
    'intermediate_table_c': """\
        SELECT
            'SOME_VALUE' AS constant_field,
            field_c1 || 'MORE_TEXT' AS c1,
            field_c2 AS c2
        FROM schema_1.source_table_c
    """,
    'dest_table_c': """\
        SELECT
            *
        FROM schema_1.intermediate_table_c
    """,
    'dest_table_d': """\
        SELECT
            *
        FROM schema_1.intermediate_table_c
    """,
    'dest_table_e': """\
        SELECT
            *
        FROM schema_1.dest_table_b
    """
}

P = 'test_db.public.'
S = 'test_db.schema_1.'

DEST_TABLES = [
    S + 'dest_table_a',
    S + 'dest_table_b',
    S + 'dest_table_c',
    S + 'dest_table_d'
]


class TransformTests(unittest.TestCase):
    def build_graph(self):
        traced = SqlTrace.trace_sql(SQLs, dialect=PostgresExtended, schema=DictSchema(TABLES), catalog='test_db', db='schema_1')
        return traced.to_graph()

    @staticmethod
    def sources(g, node_id):
        """the ids of the nodes feeding directly into node_id"""
        return {src for src, _ in g.g.in_edges(node_id)}

    @staticmethod
    def destinations(g, node_id):
        """the ids of the nodes node_id feeds directly into"""
        return {dest for _, dest in g.g.out_edges(node_id)}

    @staticmethod
    def tables_in(g):
        return {a['table'] for _, a in g.g.nodes(data=True) if a['type'] == 'column'}

    def test_source_transform(self):
        print(SqlTrace.trace_sql(SQLs, dialect=PostgresExtended, schema=DictSchema(TABLES), catalog='test_db', db='schema_1'))

        g = self.build_graph()
        g.to_agraph().draw("test_source_transform_before.png", prog="dot")

        # before the transform, dest_table_c reads from intermediate_table_c, which reads from source_table_c
        self.assertEqual({S + 'intermediate_table_c.c1'}, self.sources(g, S + 'dest_table_c.c1'))
        self.assertEqual({S + 'source_table_c.field_c2'}, self.sources(g, S + 'intermediate_table_c.c2'))

        t = MappingTransform(
            from_tables=DEST_TABLES,
            excluded_tables=[
                S + 'intermediate_table_c',
                P + 'default_table_a'
            ]
        )

        transformed = t.apply(g)

        A = transformed.to_agraph()
        A.draw("test_source_transform.png", prog="dot")

        # dest_table_c now maps straight to source_table_c, skipping the excluded intermediate table
        self.assertEqual({S + 'source_table_c.field_c1'}, self.sources(transformed, S + 'dest_table_c.c1'))
        self.assertEqual({S + 'source_table_c.field_c2'}, self.sources(transformed, S + 'dest_table_c.c2'))

        # ...and dest_table_b maps to source_table_b
        self.assertEqual({S + 'source_table_b.field_b1'}, self.sources(transformed, S + 'dest_table_b.b1'))
        self.assertEqual({S + 'source_table_b.field_b2'}, self.sources(transformed, S + 'dest_table_b.b2'))

        # dest_table_d has exactly the same SQL as dest_table_c, and is mapped in the same way
        self.assertEqual({S + 'source_table_c.field_c1'}, self.sources(transformed, S + 'dest_table_d.c1'))
        self.assertEqual({S + 'source_table_c.field_c2'}, self.sources(transformed, S + 'dest_table_d.c2'))

        # the excluded tables are gone from the result
        self.assertNotIn(S + 'intermediate_table_c', self.tables_in(transformed))
        self.assertNotIn(P + 'default_table_a', self.tables_in(transformed))
        self.assertFalse([n for n in transformed.g.nodes if n.startswith(S + 'intermediate_table_c.') and transformed.g.nodes[n]['type'] == 'column'])

        # dest_table_a only reads from an excluded table, so its columns have no source left
        self.assertIn(S + 'dest_table_a.field_1', transformed.g.nodes)
        self.assertEqual(set(), self.sources(transformed, S + 'dest_table_a.field_1'))
        self.assertEqual(set(), self.sources(transformed, S + 'dest_table_a.field_2'))

        # tables that weren't mapped (dest_table_e) are not part of the result
        self.assertNotIn(S + 'dest_table_e', self.tables_in(transformed))

        # every edge ends at a column of one of the mapped tables
        for src, dest in transformed.g.edges:
            self.assertIn(transformed.g.nodes[dest]['table'], DEST_TABLES)

    def test_source_transform_constants(self):
        g = self.build_graph()
        t = MappingTransform(
            from_tables=DEST_TABLES,
            excluded_tables=[S + 'intermediate_table_c', P + 'default_table_a']
        )
        transformed = t.apply(g)

        # a constant stays a constant: it becomes the direct source of the mapped column
        (src_b,) = self.sources(transformed, S + 'dest_table_b.constant_field')
        self.assertEqual('constant', transformed.g.nodes[src_b]['type'])
        self.assertEqual('NULL', transformed.g.nodes[src_b]['constant'])

        # a constant defined in the excluded intermediate table is carried through to the mapped table
        (src_c,) = self.sources(transformed, S + 'dest_table_c.constant_field')
        self.assertEqual('constant', transformed.g.nodes[src_c]['type'])
        self.assertEqual("'SOME_VALUE'", transformed.g.nodes[src_c]['constant'])

    def test_without_excluded_tables_maps_to_the_first_unmapped_table(self):
        g = self.build_graph()
        transformed = MappingTransform(from_tables=DEST_TABLES).apply(g)

        # nothing is excluded, so the intermediate table is kept as the source
        self.assertEqual({S + 'intermediate_table_c.c1'}, self.sources(transformed, S + 'dest_table_c.c1'))
        self.assertEqual({S + 'intermediate_table_c.c2'}, self.sources(transformed, S + 'dest_table_c.c2'))
        self.assertEqual({S + 'intermediate_table_c.constant_field'}, self.sources(transformed, S + 'dest_table_c.constant_field'))
        self.assertEqual({P + 'default_table_a.field_1'}, self.sources(transformed, S + 'dest_table_a.field_1'))

        # ...and nothing beyond it (source_table_c) is pulled in
        self.assertNotIn(S + 'source_table_c', self.tables_in(transformed))

    def test_to_tables_limits_the_tables_that_can_be_sources(self):
        g = self.build_graph()
        transformed = MappingTransform(
            from_tables=[S + 'dest_table_c'],
            to_tables=[S + 'source_table_c']
        ).apply(g)

        self.assertEqual({S + 'source_table_c.field_c1'}, self.sources(transformed, S + 'dest_table_c.c1'))
        self.assertEqual({S + 'source_table_c.field_c2'}, self.sources(transformed, S + 'dest_table_c.c2'))
        self.assertEqual({S + 'dest_table_c', S + 'source_table_c'}, self.tables_in(transformed))

    def test_node_ids_selects_which_columns_are_mapped(self):
        g = self.build_graph()
        transformed = MappingTransform(
            node_ids=[S + 'dest_table_c.c1'],
            excluded_tables=[S + 'intermediate_table_c']
        ).apply(g)

        self.assertEqual({S + 'source_table_c.field_c1'}, self.sources(transformed, S + 'dest_table_c.c1'))
        # other columns of the same table weren't selected, so they're not part of the result
        self.assertNotIn(S + 'dest_table_c.c2', transformed.g.nodes)

    def test_dest_direction_maps_sources_to_their_destinations(self):
        g = self.build_graph()
        transformed = MappingTransform(
            from_tables=[S + 'source_table_c'],
            excluded_tables=[S + 'intermediate_table_c'],
            direction=MappingTransform.DEST
        ).apply(g)

        # source_table_c now feeds dest_table_c directly, skipping the excluded intermediate table
        self.assertIn(S + 'dest_table_c.c1', self.destinations(transformed, S + 'source_table_c.field_c1'))
        self.assertIn(S + 'dest_table_c.c2', self.destinations(transformed, S + 'source_table_c.field_c2'))
        self.assertNotIn(S + 'intermediate_table_c', self.tables_in(transformed))
