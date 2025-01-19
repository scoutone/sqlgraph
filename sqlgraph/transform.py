
class Transformer():
    def edge_transform(self, g, node_id, **node_attrs):
        return [(edge[0], None, g.nodes[edge[0]], g.edges[*edge]) for edge in g.in_edges(node_id)] \
             + [(None, edge[1], g.nodes[edge[1]], g.edges[*edge]) for edge in g.out_edges(node_id)]

class Transform():
    def __init__(self, transformers):
        self.transformers = transformers or []
        
    def apply(self, g):
        for transformer in self.transformers:
            g = g.transform(transformer.edge_transform) 
        return g
    
# class SourceTransform(Transform):
#     def __init__(self, *, node_ids=None, tables=None, source_tables=None, excluded_tables=None):
#         self.node_ids = node_ids
#         self.tables = tables
#         self.source_tables = source_tables
#         self.excluded_tables = excluded_tables
#         super().__init__(transformers=[])
#
#     def apply(self, g):
#         if self.node_ids is not None:
#             mapped_node_ids = self.node_ids
#         elif self.tables is not None:
#             mapped_node_ids = []
#             for nid in g.g.nodes:
#                 if g.g.nodes[nid]['type'] == 'column':
#                     if type(self.tables) == list:
#                         if g.g.nodes[nid]['table'] in self.tables:
#                             mapped_node_ids.append(nid)
#                     else:
#                         for t, cs in self.tables.items():
#                             if g.g.nodes[nid]['table'] == t and g.g.nodes[nid]['column'] in cs:
#                                 mapped_node_ids.append(nid)
#                                 break
#         else:
#             mapped_node_ids = g.get_dest_nodes()
#
#         def get_source_nodes(g, node_id):
#             node_attrs = g.nodes[node_id]
#             if node_attrs.get('column') == 'constant_field': 
#                 print('ause')
#             if node_attrs['type'] == 'column' and \
#                node_id not in mapped_node_ids and \
#                node_attrs['table'] not in (self.excluded_tables or []):
#                 return {node_id: g.nodes[node_id]}
#             elif node_attrs['type'] in ['constant', 'unknown']:
#                 return {node_id: g.nodes[node_id]}
#             else:
#                 columns = {}
#                 constants = {}
#                 other = {}
#                 for edge in g.in_edges(node_id):
#                     edge_sources = get_source_nodes(g, edge[0])
#                     for nid, nattr in edge_sources.items():
#                         if nattr['type'] == 'column':
#                             columns[nid] = nattr
#                         elif nattr['type'] == 'constant':
#                             constants[nid] = nattr
#                         else:
#                             other[nid] = nattr
#                 source_nodes = {}
#                 if columns:
#                     source_nodes.update(columns)
#                 elif constants:
#                     source_nodes.update(constants)
#
#                 if other:
#                     source_nodes.update(other)
#
#                 return source_nodes
#
#         def edge_transform(g, node_id, **node_attrs):
#             if node_id in mapped_node_ids:
#                 source_nodes = get_source_nodes(g, node_id)
#                 return [
#                     (source_node_id, None, source_node_attrs, {}) 
#                     for source_node_id, source_node_attrs 
#                     in source_nodes.items()
#                 ]
#
#         return g.transform(edge_transform=edge_transform)
    
class MappingTransform(Transform):
    SOURCE = 0
    DEST = 1
    
    def __init__(self, *, node_ids=None, from_tables=None, to_tables=None, excluded_tables=None, direction=SOURCE):
        self.node_ids = node_ids
        self.from_tables = from_tables #tables
        self.to_tables = to_tables #source_tables
        self.excluded_tables = excluded_tables
        self.direction = direction
        super().__init__(transformers=[])

    def apply(self, g):
        if self.node_ids is not None:
            mapped_node_ids = self.node_ids
        elif self.from_tables is not None:
            mapped_node_ids = []
            for nid in g.g.nodes:
                if g.g.nodes[nid]['type'] == 'column':
                    if type(self.from_tables) == list:
                        if g.g.nodes[nid]['table'] in self.from_tables:
                            mapped_node_ids.append(nid)
                    else:
                        for t, cs in self.from_tables.items():
                            if g.g.nodes[nid]['table'] == t and g.g.nodes[nid]['column'] in cs:
                                mapped_node_ids.append(nid)
                                break
        else:
            if self.direction == self.SOURCE:
                mapped_node_ids = g.get_dest_nodes()
            else:
                mapped_node_ids = g.get_src_nodes()
            
        def get_to_nodes(g, node_id):
            node_attrs = g.nodes[node_id]
            if node_attrs['type'] == 'column' and \
               node_id not in mapped_node_ids and \
               node_attrs['table'] not in (self.excluded_tables or []) and \
               (self.to_tables is None or node_attrs['table'] in self.to_tables):
                return {node_id: g.nodes[node_id]}
            elif node_attrs['type'] in ['constant', 'unknown']: ## not applicable for dest, but not applicable, so fine
                return {node_id: g.nodes[node_id]}
            else:
                columns = {}
                constants = {}
                other = {}
                
                if self.direction == self.SOURCE:
                    edges = g.in_edges(node_id)
                else:
                    edges = g.out_edges(node_id)

                for edge in edges:
                    edge_sources = get_to_nodes(g, edge[self.direction])
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
                to_nodes = get_to_nodes(g, node_id)
                return [
                    (
                        to_node_id if self.direction == self.SOURCE else None, 
                        to_node_id if self.direction == self.DEST else None,
                        to_node_attrs, 
                        {}
                    ) 
                    for to_node_id, to_node_attrs 
                    in to_nodes.items()
                ]
        
        return g.transform(edge_transform=edge_transform)