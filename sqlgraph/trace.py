from sqlgraph.graph import SqlGraph
from sqlglot import parse_one, exp
import os
import logging
import json
from sqlgraph import source as _src
from uuid import uuid4

_type = type

logger = logging.getLogger(__name__)


class SqlTrace():
    def __init__(self, mappings):
        self._mappings = mappings
        
    @property
    def tables(self):
        return self._mappings
    
    def table(self, table_name):
        return self._mappings[table_name]
        
    def to_graph(self, **kwargs):
        return SqlGraph(self._mappings, **kwargs)
    
    def __str__(self):
        s = ''
        for table, columns in self._mappings.items():
            if s:
                s += '\n\n'
            s += table + '\n'
            s += '------------------\n'
            for column, source in columns.items():
                s += f'  {column}: {source}\n'
        return s

        
    @classmethod
    def trace_sql(cls, sql, name=None, *, dialect=None, schema=None):
        if type(sql) == str:
            if not name:
                raise ValueError('name is required for single SQL statement')
            else:
                sql = {name: sql}
                
        mapping = {}
        for n, s in sql.items():
            t = parse_one(s, dialect=dialect)
            mapping[n] = cls.Tracer(schema=schema).trace_table(t, n)
            
        return SqlTrace(mapping)
        
    @classmethod
    def trace_file(cls, file, *, name=None, dialect=None, column_resolver=None):
        if not name:
            name = os.path.basename(file).rsplit('.', 1)[0]
        with open(file) as f:
            sql = f.read()
            return cls.trace_sql(
                sql, 
                name,
                dialect=dialect,
                column_resolver=column_resolver
            )
    
    @classmethod
    def trace_directory(cls, directory, *, models=None, excluded_models=None, dialect=None, column_resolver=None):
        mappings = {}
        for root, dirs, files in os.walk(directory):
            for file in files:
                model = file[0:-4]
                if models is not None:
                    if model not in models:
                        continue
                elif excluded_models and model in excluded_models:
                    continue
                
                print(f'tracing {file}')
                mappings.update(
                    cls.trace_file(
                        os.path.join(root, file), 
                        name=model, 
                        dialect=dialect, 
                        column_resolver=column_resolver
                    )._mappings
                )
        return SqlTrace(mappings)
    
    @classmethod
    def list_models(cls, directory):
        models = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                models.append(file[0:-4])
        return models
    
    class Tracer():
        
        def __init__(self, *, schema=None):
            self.schema = schema
            self.column_cache = {}
        
        
        def get_comments(self, tbl):
            comments = tbl.comments or list()
            if 'with' in tbl.args:
                comments.extend(tbl.args['with'].comments or list())
            return comments
        
        
        def parse_mapping_dict(self, m):
            transforms = m.pop('transforms', None)
            if 'src' in m:
                table, column = m.pop('src').split('.')
                s = _src.ColumnSource(table, column, **m)
            elif 'sources' in m:
                s = _src.CompositeSource(sources=[self.parse_mapping_dict(mx) for mx in m['sources']], **{k:v for k,v in m.items() if k!= 'sources'})
            elif 'value' in m:
                s = _src.ConstantSource(**m)
            else:
                s = _src.Source(**m)
                
            if transforms:
                for t in reversed(transforms):
                    s = _src.TransformSource(t, s)
            return s
    
        
        def get_comment_mappings(self, tbl):
            comments = self.get_comments(tbl)
            mappings = {}
            for c in comments:
                try:
                    j = json.loads(c)
                except:
                    if 'mappings' in c:
                        logger.warning(f'ignored comment {c}')
                    j = {}
                    
                if 'mappings' in j:
                    mappings.update({dest_column: self.parse_mapping_dict(m) for dest_column, m in j['mappings'].items()})
                    logger.debug(f'loaded comment mappings: {j["mappings"]}')
                else:
                    logger.debug(f'ignored non-mapping comment {c}')
                    
            return mappings
                
        
        def trace_table(self, tbl, name=None):
            comment_mappings = self.get_comment_mappings(tbl)
            traced = self.trace_table_structure(tbl, name=name)
            mappings = {}
            # if type(tbl) == exp.Union:
            #     traced = self.get_star_cols(tbl)
            # else:
            #     traced = {}
            #     for e in tbl.expressions:
            #         st = self.trace(e)
            #         if type(st) != dict:
            #             st = {e.alias_or_name: st}
            #         traced.update(st)
                    
            for tk, tv in traced.items():
                mappings[tk] = comment_mappings.get(tk, tv)    
            return mappings
        
         
        def resolve_columns_for_table(self, t):
            table_key = f'{t.db}.{t.name}'
            if table_key not in self.column_cache:
                if self.schema:
                    self.column_cache[table_key]= self.schema.get_table(t.name, t.db, t.catalog)
                else:
                    self.column_cache[table_key] = None
            return self.column_cache[table_key].columns if self.column_cache[table_key] else None
        
         
        def find_direct(self, parent, exp_type):
            return list([x for x in parent.find_all(exp_type) if x.find_ancestor(parent.__class__) == parent])
        
         
        def get_applicable_ctes(self, select): 
            ctes = [] 
            cte_select = select
            while True:
                if cte_select.ctes:
                    ctes.extend(cte_select.ctes)
                if not cte_select.parent_select or cte_select.same_parent:
                    break
                else:
                    cte_select = cte_select.parent_select   
            return ctes
           
        
        
        def get_select_sources(self, select):
            sources = [select.args['from']]
            sources.extend(select.args.get('joins', []))
            sources = {s.alias_or_name: s for s in sources}
                    
            # check to see if any are CTEs
            for aon in sources:
                source = sources[aon]
                
                if type(source.args['this']) == exp.Table and not source.args['this'].db:
                    for cte in self.get_applicable_ctes(select):
                        if cte.alias_or_name == source.args['this'].name:
                            sources[aon] = cte
                            break
                            
            return sources
            
        
         
        def get_table(self, select, identifier):
            sources = self.get_select_sources(select)
            source = sources[identifier.alias_or_name]
            return source.args['this']
        
        
        
        def trace_table_structure(self, t, *, type=None, name=None):
            if _type(t) in [exp.From, exp.Join]:
                return self.trace_table_structure(
                    t.args['this'], 
                    name=name
                )
            elif _type(t) in [exp.Subquery, exp.CTE]:
                if _type(t) == exp.Subquery:
                    type = 'sq'
                    tag = 's'
                else:
                    type = 'cte'
                    tag = 'cte.'+t.alias
                return self.trace_table_structure(
                    t.args['this'],
                    type=type,
                    name=f'{name}.{tag}' if name else tag
                )
            elif _type(t) in [exp.Select, exp.Values]:
                columns = {}
                if '*' in t.named_selects:
                    for src in self.get_select_sources(t).values():
                        trc = self.trace_table_structure(src, name=name)
                        columns.update(trc)
                        
                for col_name, col_val in zip(t.named_selects, t.selects):
                    if col_name != '*':
                        columns[col_name] = self.trace(col_val)
                
                if type:
                    ts = _src.TableSource(name, columns, type=type)
                    columns = {c: _src.ColumnSource(ts, c) for c in columns}
                return columns
            elif _type(t) == exp.Table:
                return self.get_columns_for_table(t)
            elif _type(t) == exp.Union:
                left_cols = self.trace_table_structure(t.left, name=name)
                right_cols = self.trace_table_structure(t.right, name=name)
                combined = {}
                group_id = str(uuid4())
                for left, right in zip(left_cols.items(), right_cols.items()):
                    left_col, left_src = left
                    _, right_src = right
                    combined[left_col] = _src.UnionSource(left_src, right_src, group_id=group_id)
                return combined
            elif _type(t) == exp.Lateral:
                print('TODO: lateral not yet handled')
                return {}
            else:
                raise ValueError(f'unhandled table structure: [{_type(t)}]: {t}')
        
         
        def trace_column(self, column):
            if 'table' in column.args:
                src = self.get_table(column.parent_select, column.args['table'])
                if type(src) == exp.Table:
                    return _src.ColumnSource(src.name, column.name)
                else:
                    table_cols = self.trace_table_structure(src)
                    if column.name == '*':
                        return table_cols
                    else:
                        return table_cols[column.name]
            else:
                for s in self.get_select_sources(column.parent_select).values():
                    cols = self.trace_table_structure(s)
                    if column.name in cols:
                        return cols[column.name]
            return _src.UnknownSource(f'[Column] {column}')
    
        
         
        def get_parent_table(self, column):
            if 'table' in column.args:
                return self.get_table(column.parent_select, column.args['table'])
            else:
                for s in self.get_select_sources(column.parent_select).values():
                    cols = self.trace_table_structure(s)
                    if column.name in cols:
                        return s.args['this']
        
         
        def get_columns_for_table(self, table):
            if type(table) in [exp.CTE, exp.Subquery, exp.Values]:
                cols = {}
                for n, c in zip(table.named_selects, table.selects):
                    if n == '*':
                        sc = self.trace(c)
                        cols.update(sc)
                    else:
                        cols[n] = self.trace(c) 
                return cols
            else:
                return {
                    c: _src.ColumnSource(table.name, c)
                    for c in self.resolve_columns_for_table(table)
                }
        
         
        def trace_cte_column(self, cte, col_name):
            if col_name in cte.named_selects:
                return self.trace(cte.selects[cte.named_selects.index(col_name)])
            elif '*' in cte.named_selects:
                sc = self.get_star_cols(cte.args['this'])
                return sc.get(col_name, None)
            
        
        
         
        def get_star_cols(self, select):
            return self.trace_table_structure(select)
                
         
        def trace_window(self, w):
            if type(w.this) == exp.RowNumber:
                return _src.ConstantSource('ROW_NUMBER')
            elif w.this.alias_or_name == 'DENSE_RANK':
                return _src.ConstantSource(w.this.alias_or_name)
            return _src.UnknownSource('[Window] '+str(w))
        
         
        def get_src_columns(self, t):
            if 'src_columns' in t:
                return t['src_columns']
            elif 'src_column' in t:
                return [t['src_column']]
            else:
                return []
        
         
        def trace_dpipe(self, p):
            r = self.trace(p.this)
            if type(p.expression) not in [exp.Literal]:
                et = self.trace(p.expression)
                if type(r) == _src.TransformSource and r.transform == 'CONCAT':
                    sources = r.sources + [et]
                else:
                    sources = [r, et]
                r = _src.TransformSource('CONCAT', sources)
            return r
        
         
        def trace_function_call(self, d):
            name = d.name or d.key.upper()
            params = list(d.expressions) if d.expressions else [d.args['this']]
            
            if name.lower() == 'phone_priority':
                priority = {'1': 'first', '2': 'second', '3': 'third'}[str(params[0])]
                return _src.TransformSource(
                    'PHONE_PRIORITY',
                    [self.trace(p) for p in params[1:]],
                    notes=f'{priority} populated value'
                )
            elif name.lower() == 'max_length':
                return _src.TransformSource(f'MAX_LENGTH({params[1]})', self.trace(params[0]))
            elif name.lower() == 'join_valued':
                return _src.TransformSource('JOIN_VALUED', [self.trace(p) for p in params[1:]])
            elif name.lower() == 'substring':
                args = str(d.args['start'])
                if 'length' in d.args:
                    args = args+','+str(d.args['length'])
                return _src.TransformSource(f'SUBSTRING({args})', self.trace(d.args['this']))
            elif name.lower() == 'splitpart':
                args = str(d.args['delimiter'])+','+str(d.args['part_index'])
                return _src.TransformSource(f'SPLIT_PART({args})', self.trace(d.args['this']))
            elif name.lower() == 'split_max':
                args = str(d.expressions[1])+','+str(d.expressions[2])
                return _src.TransformSource(f'SPLIT_MAX({args})', self.trace(d.expressions[0]))
            elif name.lower() == 'time_to_str':
                args = str(d.args['delimiter'])+','+str(d.args['part_index'])
                return _src.TransformSource(f'TIME_TO_STR({args})', self.trace(d.args['this']))
            elif name.lower() == 'try_cast':
                return _src.TransformSource('TRY_CAST', d.expressions[0])
            elif len(params) == 1:
                return _src.TransformSource(name, self.trace(params[0]))
            
            return _src.UnknownSource(f'dot: {d}')
        
         
        def trace_coalesce(self, coalesce):
            return _src.CompositeSource(
                [
                    self.trace(x) 
                    for x in [
                        coalesce.args['this']
                    ]+coalesce.args['expressions']
                ]
            )
            
        
        def trace_struct(self, struct):
            return _src.CompositeSource(
                [
                    _src.StructSource(e.name, self.trace(e.expression))
                    for e in struct.expressions
                ]
            )
        
         
        def trace(self, e):
            if type(e) == exp.Identifier:
                return self.trace(e.parent)
            elif type(e) == exp.Column:
                return self.trace_column(e)
            elif type(e) == exp.Alias:
                return self.trace(e.args['this'])
            elif type(e) == exp.Cast:
                return self.trace(e.args['this'])
            elif type(e) == exp.Window:
                return self.trace_window(e)
            elif type(e) == exp.Coalesce:
                return self.trace_coalesce(e)
            elif type(e) == exp.Struct:
                return self.trace_struct(e)
            elif type(e) == exp.DPipe:
                return self.trace_dpipe(e)
            elif type(e) == exp.Dot:
                return self.trace_function_call(e.args['expression'])
            elif type(e) in [exp.Trim, exp.Max, exp.Upper, exp.Substring, exp.SplitPart, exp.TimeToStr]:
                return self.trace_function_call(e)
            elif type(e) == exp.Star:
                return self.get_star_cols(e.parent_select)
            elif type(e) == exp.Bracket:
                col_src = self.trace(e.args['this'])
                if type(col_src) == _src.ColumnSource:
                    return _src.TransformSource(f'LIST_INDEX[{e.output_name}]', col_src)
                return col_src
            elif type(e) in [exp.Literal, exp.CurrentDate, exp.Boolean]:
                return _src.ConstantSource(str(e))
            elif type(e) == exp.Count:
                return _src.ConstantSource('COUNT')
            elif type(e) == exp.Null:
                return _src.ConstantSource('NULL')
            elif isinstance(e, _src.Source):
                raise ValueError('something is wrong')
            elif type(e) == exp.Lower:
                return _src.TransformSource('LOWER', self.trace(e.args['this']))
            elif type(e) == exp.Not:
                return _src.TransformSource('NOT', self.trace(e.args['this']))
            elif type(e) in [exp.EQ]:
                return _src.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(e.left), 
                        self.trace(e.right)
                    ]
                )
            elif type(e) == exp.Is:
                if type(e.right) == exp.Null:
                    return _src.TransformSource('IS NULL', [self.trace(e.left)])
                else:
                    return _src.TransformSource(
                        e.__class__.__name__.upper(), 
                        [
                            self.trace(e.left), 
                            self.trace(e.right)
                        ]
                    )
            else:
                return _src.UnknownSource(f'[{type(e).__name__}] {e}')