import unittest
from sqlgraph.graph import SqlGraph
from networkx.classes.digraph import DiGraph
        
        
def create_graph(nodes=None, edges=None):
    g = DiGraph(edges)
    for node in (nodes or []):
        g.add_node(node)
    return g
        
class FilterTests(unittest.TestCase):
    def test_filter_nodes(self):
        mg = create_graph(
            nodes=None,
            edges=[
                ['node1', 'node2'],
                ['node2', 'node3'],
                ['node3', 'node4'],
                ['node4', 'node5'],
            ]
        )
        
        node_filter = lambda g, node_id, **node_attrs: node_id in ['node1', 'node3', 'node5']
        
        filtered = SqlGraph.filter_graph(mg, node_filter)
        self.assertEqual(
            ['node1', 'node3', 'node5'], 
            list(filtered.nodes.keys())
        )
        self.assertEqual(
            [
                ('node1', 'node3'),
                ('node3', 'node5')
            ],
            list(filtered.edges)
        )
        
    def test_filter_edges(self):
        mg = create_graph(
            nodes=None,
            edges=[
                ('node1', 'node3'),
                ('node2', 'node3'),
                ('node3', 'node4'),
                ('node3', 'node5'),
            ]
        )
        
        edge_filter = lambda g, u_id, v_id, **node_attrs: [u_id, v_id] != ['node1', 'node3']
        
        filtered = SqlGraph.filter_graph(
            mg, 
            edge_filter=edge_filter
        )
        
        self.assertEqual(
            ['node1', 'node2', 'node3', 'node4', 'node5'], 
            sorted(filtered.nodes.keys())
        )
        self.assertEqual(
            [
                ('node2', 'node3'),
                ('node3', 'node4'),
                ('node3', 'node5'),
            ],
            sorted(filtered.edges)
        )
        
    def test_filter_edges_and_nodes(self):
        mg = create_graph(
            nodes=None,
            edges=[
                ('node1', 'node3'),
                ('node2', 'node3'),
                ('node3', 'node4'),
                ('node3', 'node5'),
            ]
        )
        
        edge_filter = lambda g, u_id, v_id, **node_attrs: [u_id, v_id] != ['node1', 'node3']
        node_filter = lambda g, node_id, **node_attrs: node_id != 'node3'
        
        filtered = SqlGraph.filter_graph(
            mg, 
            node_filter=node_filter,
            edge_filter=edge_filter
        )
                
        self.assertEqual(
            ['node1', 'node2', 'node4', 'node5'], 
            sorted(filtered.nodes.keys())
        )
        self.assertEqual(
            [
                ('node2', 'node4'),
                ('node2', 'node5'),
            ],
            sorted(filtered.edges)
        )