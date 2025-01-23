from networkx.classes.digraph import DiGraph
import networkx as nx
from sqlgraph import model as mdl
import textwrap
from copy import deepcopy
from sqlgraph.model import TableSource, Table
import logging
import json

logger = logging.getLogger(__name__)

_type = type

DISPLAY_SETTINGS = {
    'composite': {
        'label_attribute': 'name',
        'style': 'filled',
        'fillcolor': '#b0f1b4',
        'shape': 'hexagon'
    },
    'transform': {
        'label_attribute': 'name',
        'style': 'filled',
        'fillcolor': '#ffb366',
        'shape': 'hexagon'
    },
    'conditional': {
        'label': 'COND',
        'style': 'filled',
        'fillcolor': '#CCCCFF',
        'shape': 'hexagon'
    },
    'comparison': {
        'label_attribute': 'name',
        'style': 'filled',
        'fillcolor': '#CCCCFF',
        'shape': 'hexagon'
    },
    'union': {
        'label': 'UNION',
        'style': 'filled',
        'fillcolor': '#66ffcc',
        'shape': 'hexagon'
    },
    'struct': {
        'label': 'STRUCT',
        'style': 'filled',
        'fillcolor': '#767ece',
        'shape': 'box'
    },
    'column': {
        'shape': 'box',
    },
    'constant': {
        'shape': 'cds',
        'label_attribute': 'constant',
        'style': 'filled',
        'fillcolor': '#e6e6e6', 
    },
    'unknown': {
        'style': 'filled',
        'fillcolor': '#ffffb3',
        'label_attribute': 'UNKNOWN',
    },
    'table': {
        'style': 'filled',
        'fillcolor': '#b3e0ff',
    }
}

class SqlGraph():

    def __init__(self, tables=None, *, table_group=None):
        self.g = DiGraph()
        self.tables = {}
        if tables:
            for table in tables.values():
                self.add_table(table, table_group)
            #self.add_mappings(mappings, table_group=table_group) 
            
    def to_dict(self):
        return {
            'nodes': {
                node_id: self.g.nodes[node_id]
                for node_id in self.g.nodes
            },
            'edges': [
                {
                    'vertices': list(edge),
                    'attributes': self.g.edges[edge]
                }
                for edge in self.g.edges
            ]
        }
        
    def to_file(self, filename):
        d = self.to_dict()
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    def from_dict(self, d):
        for node_id, node_attributes in d.get('nodes', {}).items():
            self.g.add_node(node_id, **node_attributes)
            
        for edge in d.get('edges', []):
            self.g.add_edge(*edge['vertices'], **edge.get('attributes', {}))
            
        return self
    
    def from_file(self, filename):
        with open(filename) as f:
            return self.from_dict(json.load(f))
        
        
    def add_all(self, other):
        self.g = nx.compose(self.g, other.graphs)
        
    @staticmethod
    def intersects(l1, l2):
        return len([l for l in l1 if l in l2]) > 0

    @staticmethod
    def _sub_graph(cls, g, src_nodes, dest_nodes):
        included_edges = set()
        included_nodes = set()
        for src_id in src_nodes:
            for dest_id in dest_nodes:
                for p in nx.all_simple_edge_paths(g, src_id, dest_id):
                    for edge in p:
                        included_nodes.add(edge[0])
                        included_nodes.add(edge[1])
                        included_edges.add(str(edge))
                    
        def node_filter(node_id):
            return node_id in included_nodes
        
        return g.subgraph(
                nx.subgraph_view(
                    g, 
                    filter_node=node_filter,
                    filter_edge=lambda x,y: str([x,y]) in included_edges
                )
            )
        
    @classmethod 
    def transform_graph(cls, g, edge_transform):
        tg = nx.DiGraph()
        for node_id in g.nodes:
            edge_data = edge_transform(g, node_id, **g.nodes[node_id])
            if edge_data is not None:
                tg.add_node(node_id, **g.nodes[node_id])
                for source_node_id, dest_node_id, other_node_attrs, edge_attrs in edge_data:
                    if source_node_id:
                        tg.add_node(source_node_id, **other_node_attrs)
                        tg.add_edge(source_node_id, node_id, **edge_attrs)
                    else:
                        tg.add_node(dest_node_id, **other_node_attrs)
                        tg.add_edge(node_id, dest_node_id, **edge_attrs)
        return tg
    
    def transform(self, edge_transform):
        sg = SqlGraph()
        sg.g = SqlGraph.transform_graph(self.g, edge_transform)
        return sg
    
    def get_dest_nodes(self):
        return [node_id for node_id in self.g.nodes if len(self.g.out_edges(node_id)) == 0]
    
    def get_src_nodes(self):
        return [node_id for node_id in self.g.nodes if len(self.g.in_edges(node_id)) == 0]
        
    @classmethod
    def filter_graph(cls, g, node_filter=None, edge_filter=None):
        fg = g.copy()
        
        if edge_filter:
            for edge in list(g.edges):
                if not edge_filter(g, *edge, **g.edges[*edge]):
                    fg.remove_edge(*edge)
        
        if node_filter:
            for node_id in list(fg.nodes):
                node_attrs = fg.nodes[node_id]
                if not node_filter(g, node_id, **node_attrs):
                    in_edges = list(fg.in_edges(node_id))
                    out_edges = list(fg.out_edges(node_id))
                    for in_edge in in_edges:
                        for out_edge in out_edges:
                            fg.add_edge(in_edge[0], out_edge[1])
                    for in_edge in in_edges:
                        fg.remove_edge(*in_edge)
                    for out_edge in out_edges:
                        fg.remove_edge(*out_edge)
                    fg.remove_node(node_id)
                else:
                    print(f'keep {node_id}')
        return fg
    
    def filter(self, node_filter=None, edge_filter=None):
        sg = SqlGraph()
        sg.g = SqlGraph.filter_graph(
            self.g,
            node_filter,
            edge_filter
        )
        return sg

    def get_nodes_in_groups(self, table_groups):
        if table_groups and type(table_groups) != list:
            table_groups = [table_groups]
        
        return [
            node_id 
            for node_id, attrs 
            in nx.get_node_attributes(
                self.g, 'groups', []
            ).items() 
            if SqlGraph.intersects(table_groups, attrs)
        ]
        
    
    def add_table_group(self, table_group, tables):
        for node_id in self.g.nodes:
            node = self.g.nodes[node_id]
            if node['type'] == 'column':
                for group_table_id in tables:
                    if Table.ids_match(group_table_id, node['table']):
                        if type(tables) != dict or node['column'] in tables[group_table_id]:
                            node.setdefault('groups', []).append(table_group)
        
    def add_table(self, table, table_group=None):
        for column in table.columns:
            src = table.sources[column] if _type(table) == TableSource else None
            self._add_column(table, column, src)
            
        if table_group:
            self.add_table_group(table_group, table.name)
        
    # def add_mappings(self, mappings, *, table_group=None):
    #     for table, cols in mappings.items():
    #         for column, source in cols.items():
    #             self._add_column_source(table, column, source)
    #     if table_group:
    #         self.add_table_group(table_group, mappings.keys())
            
    def _apply_display_settings(self, node):
        display_settings = DISPLAY_SETTINGS.get(node['type'])
        if display_settings:
            node = deepcopy(node)
            label_attribute = display_settings.get('label_attribute')
            display_settings = {k: v for k,v in display_settings.items() if k not in ['label_attribute']}
            if label_attribute:
                node['label'] = node[label_attribute]
            node.update(display_settings)
        return node
                
    def _add_column(self, table, column, source):
        node = {
            'id': f'{table}.{column}',
            'type': 'column',
            'table': table.id,
            'table_type': table.type,
            'column': column,
            'mapped': True
        }
        
        node = self._apply_display_settings(node)
        node_id = self._add_node(node)
        
        if source:
            self._add_node_source(node_id, source)

    def _add_node(self, node):
        logger.debug(f'ADD NODE: {node["id"]}')
        self.g.add_node(node['id'], **{k: v for k,v in node.items() if k != 'id'})
        return node['id']
    
    def _add_edge(self, src_node, dest_node, **attributes):
        logger.debug(f'ADD EDGE: {src_node}->{dest_node}')
        self.g.add_edge(src_node, dest_node, **attributes)
        
    def _add_node_source(self, dest_id, source, *, seq=None, edge_label=None):
        if type(source) == mdl.PathSource:
            return self._add_node_source(dest_id, source.source, edge_label=source.path)

        additional_attributes = {}
        if type(source) == mdl.ColumnSource:
            if isinstance(source.table, Table):
                if source.table.id not in self.tables:
                    for c in source.table.columns:
                        self._add_column(
                            source.table, 
                            c, 
                            source.table.sources[c] if _type(source.table) == TableSource else None
                        )
                    self.tables[source.table.id] = source.table
                additional_attributes = {'table_type': source.table.type}
                source.table = source.table.id
            else:
                additional_attributes = {'table_type': 'table'}
                
        
        
        if type(source) == mdl.ColumnSource:
            src_id =  f'{source.table}.{source.column}'
        else:
            src_id = f'{dest_id}.source'
            if edge_label is not None:
                src_id += f'.{edge_label}'
            elif seq is not None:
                src_id += f'.[{seq}]'
        
        src_attributes = {k: v for k,v in source.to_dict().items() if k != 'sources'}
        
        src_node = {'id': src_id, **src_attributes, **additional_attributes}
        
        src_node = self._apply_display_settings(src_node)
        
        self._add_node(src_node)
        
        edge_attrs = {
            'seq': seq,
            'notes': source.notes,
        }
        if edge_label:
            edge_attrs['label'] = edge_label
        self._add_edge(src_id, dest_id, **edge_attrs)
        
        if hasattr(source, 'sources'):
            if _type(source.sources) == dict:
                for sn, ss in source.sources.items():
                    self._add_node_source(src_id, ss, edge_label=sn)
            else:
                for i in range(len(source.sources)):
                    self._add_node_source(src_id, source.sources[i], seq=i, edge_label=f'[{i}]')
        
        if hasattr(source, 'source'):
            self._add_node_source(src_id, source.source)

        if _type(source) == mdl.ComparisonSource:
            self._add_node_source(src_id, source.left, edge_label='LEFT')
            self._add_node_source(src_id, source.right, edge_label='RIGHT')

        if _type(source) == mdl.ConditionalSource:
            self._add_node_source(src_id, source.condition, edge_label='IF')
            self._add_node_source(src_id, source.true_value, edge_label='THEN')
            self._add_node_source(src_id, source.false_value, edge_label='ELSE')
        
    def get_source_graph(self, node_id, table_groups=None):
        if table_groups and _type(table_groups) != list:
            table_groups = [table_groups]  
            
        sg = self.g.subgraph(nx.ancestors(self.g, node_id) | {node_id}).copy()

        if table_groups:
            for n in sg.nodes:
                if SqlGraph.intersects(table_groups, sg.nodes[n].get('groups', [])):
                    #remove everything upstream
                    for edge in list(sg.in_edges(n)):
                        sg.remove_edge(*edge)
            sg = sg.subgraph(nx.ancestors(sg, node_id) | {node_id}).copy()
        return sg
        
    def get_dest_graph(self, node_id, table_groups=None):
        if table_groups and _type(table_groups) != list:
            table_groups = [table_groups]  
            
        sg = self.g.subgraph(nx.descendants(self.g, node_id) | {node_id}).copy()

        if table_groups:
            for n in sg.nodes:
                if SqlGraph.intersects(table_groups, sg.nodes[n].get('groups', [])):
                    #remove everything upstream
                    for edge in list(sg.out_edges(n)):
                        sg.remove_edge(*edge)
            sg = sg.subgraph(nx.descendants(sg, node_id) | {node_id}).copy()
        return sg


                        
                    
    
    @classmethod
    def to_str(cls, g=None, *, node_id=None, src_id=None, dest_id=None):
        if not g:
            g = self.g
            
        if src_id and dest_id:
            # edge
            dest_ids = None
        elif dest_id:
            dest_ids = [dest_id]
        elif node_id:
            dest_ids = [node_id]
        else:
            dest_ids = [node_id for node_id in g.nodes if len(g.out_edges(node_id)) == 0]
        
        rows = []
        if dest_ids:
            # initial dest node
            for dest_id in dest_ids:
                rows.append(f'- {dest_id}')
                for u,v in g.in_edges(dest_id):
                    rows.append(
                        textwrap.indent(
                            SqlGraph.to_str(g, src_id=u, dest_id=v), 
                            '  '
                        )
                    )
        else:
            # edges
            if g.nodes[src_id]["type"] == 'union':
                rows.append(f'- UNION')
            else:
                rows.append(f'- {src_id} [{g.nodes[src_id]["type"]}]')
            for u,v in g.in_edges(src_id):
                rows.append(
                    textwrap.indent(
                        SqlGraph.to_str(g, src_id=u, dest_id=v),
                        '  '
                    )
                )
                
        return '\n'.join(rows)
    
    @classmethod
    def sort_dict(cls, d):
        return {k: d[k] for k in sorted(d.keys())}
        
    
    def get_group_source_mapping(self, dest_groups, *, src_groups=None, excluded_groups=None):
        mapping = {}
        for node_id in self.get_nodes_in_groups(dest_groups):
            node = self.g.nodes[node_id]
            mapping.setdefault(node['table'], {})[node['column']] = self.get_source_mapping(
                node_id, 
                dest_groups=dest_groups, 
                src_groups=src_groups, 
                excluded_groups=excluded_groups
            )
        
        mapping = SqlGraph.sort_dict(
            {
                k: SqlGraph.sort_dict(v) 
                for k,v in mapping.items()
            }
        )
            
        return mapping
    
    def get_source_mapping(self, node_id, *, dest_groups=None, src_groups=None, excluded_groups=None):
        sg = self.get_source_graph(node_id, src_groups)
        mapped = {}
        constants = []
        unknowns = []
        if src_groups:
            for sgn_id in sg.nodes:
                if SqlGraph.intersects(src_groups, sg.nodes[sgn_id].get('groups', [])):
                    mapped[sgn_id] = self.get_source_path_attributes(node_id, sgn_id, g=sg)
        else:
            for sgn_id in sg.nodes:
                if sgn_id == node_id:
                    continue
                if sg.nodes[sgn_id]['type'] == 'column' and \
                   sg.nodes[sgn_id].get('table_type') not in ['sq', 'cte'] and \
                   not SqlGraph.intersects(dest_groups or [], sg.nodes[sgn_id].get('groups', [])) and \
                   not SqlGraph.intersects(excluded_groups or [], sg.nodes[sgn_id].get('groups', [])):
                    mapped[sgn_id] = self.get_source_path_attributes(node_id, sgn_id, g=sg)
                elif sg.nodes[sgn_id]['type'] == 'constant':
                    constants.append(sg.nodes[sgn_id]['constant'])
                elif sg.nodes[sgn_id]['type'] == 'unknown':
                    unknowns.append(sg.nodes[sgn_id]['UNKNOWN'])
        
        names = [v for v in mapped.values() if v]
        if not len(names):
            mapped = list(mapped.keys())      
        
        return mapped or constants or unknowns
    
    def get_source_path_attributes(self, dest_id, src_id, *, g=None):
        if not g:
            g = self.g
        
        names = []
        cur_names = []
        for n in list(reversed(nx.shortest_path(g, src_id, dest_id))):
            if g.nodes[n]['type'] == 'struct':
                raise ValueError('check this')
                cur_names.append(g.nodes[n]['name'])
            elif g.nodes[n]['type'] == 'column':
                names = cur_names
            else:
                names = []
                
        attributes = {
            'struct_name': '.'.join(reversed(names)) if names else None
        }
        
        attributes = {k: v for k,v in attributes.items() if v is not None}
        return attributes if len(attributes) > 0 else None

    def get_group_dest_mapping(self, src_groups, *, dest_groups=None, excluded_groups=None):
        mapping = {}
        for node_id in self.get_nodes_in_groups(src_groups):
            node = self.g.nodes[node_id]
            mapping.setdefault(node['table'], {})[node['column']] = self.get_dest_mapping(
                node_id,
                src_groups=src_groups,
                dest_groups=dest_groups,
                excluded_groups=excluded_groups
            )

        mapping = SqlGraph.sort_dict(
            {
                k: SqlGraph.sort_dict(v) 
                for k,v in mapping.items()
            }
        )
            
        return mapping
    
    def get_dest_mapping(self, node_id, *, src_groups=None, dest_groups=None, excluded_groups=None):
        sg = self.get_dest_graph(node_id, dest_groups)
        mapped = {}
        if dest_groups:
            for sgn_id in sg.nodes:
                if sgn_id == node_id:
                    continue
                if SqlGraph.intersects(dest_groups, sg.nodes[sgn_id].get('groups', [])):
                    mapped[sgn_id] = self.get_dest_path_attributes(node_id, sgn_id) 
        else:
            for sgn_id in sg.nodes:
                if sgn_id == node_id:
                    continue
                if sg.nodes[sgn_id]['type'] == 'column' and \
                   sg.nodes[sgn_id].get('table_type') not in ['sq', 'cte'] and \
                   not SqlGraph.intersects(src_groups or [], sg.nodes[sgn_id].get('groups', [])) and \
                   not SqlGraph.intersects(excluded_groups or [], sg.nodes[sgn_id].get('groups', [])):
                    mapped[sgn_id] = self.get_dest_path_attributes(node_id, sgn_id) 
                    
        names = [v for v in mapped.values() if v]
        if not len(names):
            mapped = list(mapped.keys())  
            
        return mapped
    
    def get_dest_path_attributes(self, src_id, dest_id, *, g=None):
        if not g:
            g = self.g
        
        names = []
        cur_names = []
        for n in list(reversed(nx.shortest_path(g, src_id, dest_id))):
            if g.nodes[n]['type'] == 'struct':
                raise ValueError('check this')
                cur_names.append(g.nodes[n]['name'])
            elif g.nodes[n]['type'] == 'column':
                names = cur_names
            else:
                names = []
                
        attributes = {
            'struct_name': '.'.join(reversed(names)) if names else None
        }
        
        attributes = {k: v for k,v in attributes.items() if v is not None}
        return attributes if len(attributes) > 0 else None
                
    # def get_sources(self, node_id, *, type=None, table_groups=None, return_nodes=False):
    #     if table_groups and _type(table_groups) != list:
    #         table_groups = [table_groups]  
    #
    #     ancestors = self.g.subgraph(nx.ancestors(self.g, node_id))
    #
    #     if type or table_groups:
    #         def node_filter(node_id):
    #             attrs = self.g.nodes[node_id]
    #             if type:
    #                 if attrs['type'] != type:
    #                     return False
    #
    #             if table_groups:
    #                 if len([g for g in attrs.get('groups', []) if g in table_groups]) == 0:
    #                     return False
    #             return True
    #
    #         ancestors = ancestors.subgraph(
    #             nx.subgraph_view(
    #                 ancestors, 
    #                 filter_node=node_filter
    #             )
    #         )
    #
    #     source_ids = [a for a in ancestors if len(ancestors.in_edges(a)) == 0]
    #     if return_nodes:
    #         return [{'id': source_id, **self.g.nodes[source_id]} for source_id in source_ids]
    #     else:
    #         return source_ids

    def get_nodes(self, *, types=None, table_groups=None):
        if table_groups and type(table_groups) != list:
            table_groups = [table_groups]
            
        def node_filter(node_id):
            attrs = self.g.nodes[node_id]
            if types and attrs['type'] not in types:
                return False
            if table_groups and not SqlGraph.intersects(table_groups, attrs.get('groups', [])):
                return False
            return True
        return nx.subgraph_view(self.g, filter_node=node_filter).nodes
    
    # def get_columns(self, *, table_groups=None):
    #     column_nodes = self.get_nodes(types=['column'], table_groups=table_groups)
    #     tables = {}
    #     for node_id in column_nodes:
    #         node = self.g.nodes[node_id]
    #         sources = self.get_sources(node_id)
    #         if len(sources) == 0:
    #             #this is a source only table.  exclude for now
    #             pass
    #         else:
    #             tables.setdefault(node['table'], []).append(node['column'])
    #     return tables
    
    # def get_tables(self):
    #     return list(self.get_columns().keys())
    
    def get_mappings(self, *, src_groups=None, dest_groups=None):
        tables = self.get_columns(table_groups=dest_groups)
        return {
            table: {
                column: self.get_column_mapping(table, column, table_groups=src_groups)
                for column in columns
            }
            for table, columns in tables.items()
        }
        
    def get_column_mapping(self, table, column, *, table_groups=None, direction='source'):      
        column_id = f'{table}.{column}'
        column_node = self.g.nodes[column_id]
        
        column_sources = self.get_sources(
            column_id, 
            type='column', 
            return_nodes=True, 
            table_groups=table_groups
        )
        
        if len(column_sources):
            source_dicts = {
                source['id']:  {'table': source['table'], 'column': source['column']}
                for source in column_sources
            }
            
        else:
            constant_sources = self.get_column_sources(
                table, 
                column, 
                type='constant', 
                return_nodes=True
            )
            
            if len(constant_sources):
                source_dicts = {
                    source['id']:  {'value': source['constant']}
                    for source in constant_sources
                }
                
            else:
                
                all_sources = self.get_sources(
                    column_id, 
                    type=None, 
                    return_nodes=True
                )
                
                source_dicts = {
                    source['id']:  {}
                    for source in all_sources
                }
                  
        sources = []
        for source_id, source_dict in source_dicts.items():
            path = nx.shortest_path(self.g, source_id, column_id)
            notes = []
            for i in range(len(path)-1):
                edge = self.g.edges[path[i], path[i+1]]
                if 'notes' in edge and edge['notes']:
                    notes.append(edge['notes'])
            if notes:
                source_dict['notes'] = notes
            sources.append(source_dict)
            
        m = {'sources': sources}
        if 'notes' in column_node:
            m['notes'] = column_node['notes']
            
        return m
    
    def to_rows(self, *, src_groups=None, dest_groups=None):
        mappings = self.get_mappings(
            src_groups=src_groups, 
            dest_groups=dest_groups
        )
        rows = []
        for table_name, columns in mappings.items():
            for column_name, column_data in columns.items():
                source_notes = []
                source_vals = []
                for source in column_data['sources']:
                    if 'value' in source:
                        source_vals.append(f'{source["value"]}')
                    elif 'table' in source:
                        source_vals.append(f'{source["table"]}.{source["column"]}')
                    source_notes.extend(source.get('notes', []))    
                    
                rows.append( 
                    {
                        'dest_table': table_name,
                        'dest_column': column_name,
                        'source': '\n'.join(source_vals),
                        'notes': '\n'.join([x for x in [column_data.get('notes')] + source_notes if x]) or None
                    }
                )
        return rows

        
    def to_agraph(self, g=None, *, cluster_tables=True, graph_attrs={'rankdir': 'LR'}):
        if not g:
            g = self.g
        A = nx.nx_agraph.to_agraph(g)  # convert to a graphviz graph
        for k,v in graph_attrs.items():
            A.graph_attr[k] = v
        
        if cluster_tables:
            tables = {}
            for node_id in g.nodes:
                node = g.nodes[node_id]
                if node['type'] == 'column':
                    tables.setdefault(node['table'], []).append(node_id)
                    A.get_node(node_id).attr['label'] = node['column']
                    if 'style' not in node:
                        A.get_node(node_id).attr['style'] = 'filled'
                        A.get_node(node_id).attr['fillcolor'] = '#66c2ff'
                    
            for table, columns in tables.items():
                _ = A.add_subgraph(
                    columns, 
                    name=f'cluster.{table}',
                    label=table,
                    **DISPLAY_SETTINGS.get('table', {})
                )
        return A