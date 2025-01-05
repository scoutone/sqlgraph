import networkx as nx
from pickle import TRUE

class Selector():
    def select_node(self, g, node_id, **node_attrs):
        return True
        
    def select_edge(self, g, u, v, **edge_attrs):
        return True
    
class FunctionSelector(Selector):
    def __init__(self, select_node=None, select_edge=None):
        self.select_node_func = select_node
        self.select_edge_func = select_edge
        
    def select_node(self, g, node_id, **node_attrs):
        if self.select_node_func:
            return self.select_node_func(g, node_id, **node_attrs)
        else:
            return True
        
    def select_edge(self, g, u, v, **edge_attrs):
        if self.select_edge_func:
            return self.select_edge_func(g, u, v, **edge_attrs)
        else:
            return True
    
class Filter():
    def __init__(self, selectors):
        self.selectors = selectors
        
    def apply(self, g):
        for selector in self.selectors:
            g = g.filter(node_filter=selector.select_node, edge_filter=selector.select_edge)
        return g
    
class SimpleFilter(Filter):
    def __init__(self, *, source_columns=None, dest_columns=None, source_tables=None, dest_tables=None, excluded_tables=None):
        def select_node_1(g, node_id, **node_attrs):
            if node_attrs['type'] == 'column' and node_attrs['table_type'] == 'table':
                table = node_attrs['table']
                column = node_attrs['column']
                table_column = f'{table}.{column}'
                
                #if explicitly excluded, remove
                if excluded_tables and table in excluded_tables:
                    return False
                
                # if explicitly included, keep
                if table in ((source_tables or []) + (dest_tables or [])) \
                or table_column in ((source_columns or []) + (dest_columns or [])):
                    return True
                
                if source_columns:
                    valid = False
                    sg = g.subgraph(nx.ancestors(g, node_id) | {node_id}).copy()
                    for sgn_id in sg.nodes:
                        if sgn_id in source_columns:
                            valid = True
                            break
                    if not valid:
                        return False
                
                if dest_columns:
                    valid = False
                    sg = g.subgraph(nx.descendants(g, node_id) | {node_id}).copy()
                    for sgn_id in sg.nodes:
                        if sgn_id in dest_columns:
                            valid = True
                            break
                    if not valid:
                        return False
                    
                if source_tables:
                    valid = False
                    sg = g.subgraph(nx.ancestors(g, node_id) | {node_id}).copy()
                    for sgn_id in sg.nodes:
                        if sg.nodes[sgn_id].get('table') in source_tables:
                            valid = True
                            break
                    if not valid:
                        return False
                    
                if dest_tables:
                    valid = False
                    sg = g.subgraph(nx.descendants(g, node_id) | {node_id}).copy()
                    for sgn_id in sg.nodes:
                        if sg.nodes[sgn_id].get('table') in dest_tables:
                            valid = True
                            break
                    if not valid:
                        return False

                return True
            
            if node_attrs['type'] == 'constant':
                return True
            
            if node_attrs['type'] == 'unknown':
                return True
            
            return False
            
        def select_node_2(g, node_id, **node_attrs):
            print(node_id)
            if g.in_edges(node_id) or g.out_edges(node_id):
                if node_attrs['type'] == 'constant':
                    for _, dest_node_id in g.out_edges(node_id):
                        # if there is a non-constant source, remove the constant source
                        for src_node_id, _ in g.in_edges(dest_node_id):
                            if g.nodes[src_node_id]['type'] != 'constant':
                                return False
                    # no non-constant sources found, keep the constant
                    return True
                else:
                    return True
            elif g.nodes[node_id].get('table') in ((source_tables or [])+(dest_tables or [])) \
              or node_id in ((source_columns or []) + (dest_columns or [])):
                return True
            else:
                return False
            
        super().__init__(
            [
                FunctionSelector(select_node=select_node_1),
                FunctionSelector(select_node=select_node_2)
            ]
        )
                
                
                
        