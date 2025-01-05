
class Transformer():
    def edge_transform(self, g, node_id, **node_attrs):
        return g.in_edges(node_id) + g.out_edges(node_id)

class Transform():
    def __init__(self, transformers):
        self.transformers = transformers or []
        
    def apply(self, g):
        for transformer in self.transformers:
            g = g.transform(transformer.edge_transform) 
        return g
    
class SourceTransform(Transform):
    def __init__(self, *, node_ids=None, tables=None, source_tables=None, excluded_tables=None):
        self.node_ids = node_ids
        self.tables = tables
        self.source_tables = source_tables
        self.excluded_tables = excluded_tables
        super().__init__(transformers=[])

    def apply(self, g):
        if self.node_ids is not None:
            mapped_node_ids = self.node_ids
        elif self.tables is not None:
            mapped_node_ids = []
            for nid in g.g.nodes:
                if g.g.nodes[nid]['type'] == 'column':
                    if type(self.tables) == list:
                        if g.g.nodes[nid]['table'] in self.tables:
                            mapped_node_ids.append(nid)
                    else:
                        for t, cs in self.tables.items():
                            if g.g.nodes[nid]['table'] == t and g.g.nodes[nid]['column'] in cs:
                                mapped_node_ids.append(nid)
                                break
        else:
            mapped_node_ids = g.get_dest_nodes()
            
        def get_source_nodes(g, node_id):
            node_attrs = g.nodes[node_id]
            if node_attrs.get('column') == 'constant_field': 
                print('ause')
            if node_attrs['type'] == 'column' and \
               node_id not in mapped_node_ids and \
               node_attrs['table'] not in (self.excluded_tables or []):
                return {node_id: g.nodes[node_id]}
            elif node_attrs['type'] in ['constant', 'unknown']:
                return {node_id: g.nodes[node_id]}
            else:
                columns = {}
                constants = {}
                other = {}
                for edge in g.in_edges(node_id):
                    edge_sources = get_source_nodes(g, edge[0])
                    for nid, nattr in edge_sources.items():
                        if nattr['type'] == 'column':
                            columns[nid] = nattr
                        elif nattr['type'] == 'constant':
                            constants[nid] = nattr
                        else:
                            other[nid] = nattr
                source_nodes = {}
                if columns:
                    source_nodes.update(columns)
                elif constants:
                    source_nodes.update(constants)
                
                if other:
                    source_nodes.update(other)
                
                return source_nodes
                
        def edge_transform(g, node_id, **node_attrs):
            if node_id in mapped_node_ids:
                source_nodes = get_source_nodes(g, node_id)
                return [
                    (source_node_id, source_node_attrs, {}) 
                    for source_node_id, source_node_attrs 
                    in source_nodes.items()
                ]
        
        return g.transform(edge_transform=edge_transform)