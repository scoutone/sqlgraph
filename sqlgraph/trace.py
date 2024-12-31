from sqlgraph.graph import SqlGraph
from sqlglot import parse_one, exp
import os
import logging
import json
from sqlgraph import model as mdl
from uuid import uuid4

_type = type

logger = logging.getLogger(__name__)


class SqlTrace():
    def __init__(self, tables):
        self.tables = tables
        
    # @property
    # def tables(self):
    #     return self._mappings
    
    def table(self, table_name):
        return self.tables[table_name]
        
    def to_graph(self, **kwargs):
        return SqlGraph(self.tables, **kwargs)
    
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
    def trace_sql(cls, sql, name=None, *, dialect=None, schema=None, db=None, catalog=None, tracers=None):
        if type(sql) == str:
            if not name:
                raise ValueError('name is required for single SQL statement')
            else:
                sql = {name: sql}
                
        tables = {}
        for n, s in sql.items():
            tbl = mdl.Table(n, None, db, catalog)
            t = parse_one(s, dialect=dialect)
            tables[n] = cls.Tracer(schema=schema, tracers=tracers).trace_table(t, tbl.id)
            
        return SqlTrace(tables)
        
    @classmethod
    def trace_file(cls, file, *, name=None, **kwargs):
        if not name:
            name = os.path.basename(file).rsplit('.', 1)[0]
        with open(file) as f:
            sql = f.read()
            return cls.trace_sql(
                sql, 
                name,
                **kwargs
            )
    
    @classmethod
    def trace_directory(cls, directory, *, models=None, excluded_models=None, **kwargs):
        tables = {}
        for root, dirs, files in os.walk(directory):
            for file in files:
                model = file[0:-4]
                if models is not None:
                    if model not in models:
                        continue
                elif excluded_models and model in excluded_models:
                    continue
                
                print(f'tracing {file}')
                tables.update(
                    cls.trace_file(
                        os.path.join(root, file), 
                        name=model, 
                        **kwargs
                    ).tables
                )
        return SqlTrace(tables)
    
    @classmethod
    def list_models(cls, directory):
        models = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                models.append(file[0:-4])
        return models
    
    class Tracer():
        
        def __init__(self, *, schema=None, tracers=None):
            self.schema = schema
            self.column_cache = {}
            self.tracers = tracers or {}
        
        
        def get_comments(self, tbl):
            comments = tbl.comments or list()
            if 'with' in tbl.args:
                comments.extend(tbl.args['with'].comments or list())
            return comments
        
        
        def parse_mapping_dict(self, m):
            transforms = m.pop('transforms', None)
            if 'src' in m:
                table, column = m.pop('src').split('.')
                s = mdl.ColumnSource(table, column, **m)
            elif 'sources' in m:
                s = mdl.CompositeSource(sources=[self.parse_mapping_dict(mx) for mx in m['sources']], **{k:v for k,v in m.items() if k!= 'sources'})
            elif 'value' in m:
                s = mdl.ConstantSource(**m)
            else:
                s = mdl.Source(**m)
                
            if transforms:
                for t in reversed(transforms):
                    s = mdl.TransformSource(t, s)
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
            return self.trace_table_structure(tbl, name=name, type='table')
        
         
        def resolve_table(self, t):
            table_key = f'{t.db}.{t.name}'
            if table_key not in self.column_cache:
                if self.schema:
                    self.column_cache[table_key]= self.schema.get_table(t.name, t.db or None, t.catalog or None)
                else:
                    self.column_cache[table_key] = None
            return self.column_cache[table_key]
        
         
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
            if 'from' not in select.args:
                parent = select.parent
                while _type(parent) != exp.Lateral and parent.parent and not parent.same_parent:
                    parent = parent.parent
                    
                if _type(parent) == exp.Lateral:
                    lateral = parent
                    #dealing with lateral
                    parent_sources = self.get_select_sources(lateral.parent_select)
                    prior_sources = {}
                    for pn, ps in parent_sources.items():
                        if ps.args.get('this') == lateral:
                            break
                        prior_sources[pn] = ps
                    return prior_sources
                else:
                    raise ValueError(f'unhandled select sources: {select}')
                
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
        
        def trace_table_structure(self, t, *, type=None, name=None, select_sources=None):
            if _type(t) == exp.Lateral:
                
                # parent_sources = self.get_select_sources(t.parent_select)
                # prior_sources = {}
                # for pn, ps in parent_sources.items():
                #     if ps.args.get('this') == t:
                #         break
                #     prior_sources[pn] = ps
                # print(t)
                # print('pause')
                return self.trace_table_structure(
                    t.args['this'], 
                    name=name, 
                    #select_sources=prior_sources
                )
            elif _type(t) in [exp.From, exp.Join]:
                return self.trace_table_structure(
                    t.args['this'], 
                    name=name,
                    select_sources=select_sources
                )
            elif _type(t) in [exp.Subquery, exp.CTE]:
                if _type(t) == exp.Subquery:
                    type = 'sq'
                    tag = 'sq'
                else:
                    type = 'cte'
                    tag = 'cte.'+t.alias
                return self.trace_table_structure(
                    t.args['this'],
                    type=type,
                    name=f'{name}.{tag}' if name else tag,
                    select_sources=select_sources
                )
            elif _type(t) in [exp.Select, exp.Values]:
                columns = {}
                if '*' in t.named_selects:
                    if not select_sources:
                        select_sources = self.get_select_sources(t)
                    for src in select_sources.values():
                        trc = self.trace_table_structure(src, name=name)
                        for c in trc.columns:
                            columns[c] = mdl.ColumnSource(trc, c)
                        
                for col_name, col_val in zip(t.named_selects, t.selects):
                    if col_name != '*':
                        columns[col_name] = self.trace(col_val)
                
                if not type:
                    type = 'values' if _type(t) == exp.Values else 'select'
                ts = mdl.TableSource(name, columns, type=type)
                return ts
            elif _type(t) == exp.Table:
                return self.get_columns_for_table(t, name)
            elif _type(t) == exp.Union:
                union_tables = []
                while _type(t) == exp.Union:
                    union_tables.insert(0, t.right)
                    t = t.left
                union_tables.insert(0, t)
                
                uts = list([
                    self.trace_table_structure(union_tables[i], name=name+f'.union[{i}]', select_sources=select_sources) 
                    for i in range(len(union_tables))
                ])
                
                ts = mdl.TableSource(
                    name, 
                    {
                        c: mdl.UnionSource(
                            sources=[
                                mdl.ColumnSource(t, c) 
                                for t in uts
                            ]
                        )
                        for c in uts[0].columns
                    }, 
                    type='union'
                )
                return ts
            else:
                raise ValueError(f'unhandled table structure: [{_type(t)}]: {t}')
        
         
        def trace_column(self, column):
            if 'table' in column.args:
                src = self.get_table(column.parent_select, column.args['table'])
                if type(src) == exp.Table:
                    return mdl.ColumnSource(self.get_columns_for_table(src, src.name), column.name)
                else:
                    ts = self.trace_table_structure(src)
                    if column.name == '*':
                        return {c: mdl.ColumnSource(ts, c) for c in ts.columns.keys()}
                    else:
                        return mdl.ColumnSource(ts, column.name)
            else:
                for s in self.get_select_sources(column.parent_select).values():
                    ts = self.trace_table_structure(s)
                    if column.name in ts.columns:
                        return mdl.ColumnSource(ts, column.name)
            return mdl.UnknownSource(f'[Column] {column}')
    
        
         
        def get_parent_table(self, column):
            if 'table' in column.args:
                return self.get_table(column.parent_select, column.args['table'])
            else:
                for s in self.get_select_sources(column.parent_select).values():
                    ts = self.trace_table_structure(s)
                    if column.name in ts.columns:
                        return s.args['this']
        
         
        def get_columns_for_table(self, table, name):
            if _type(table) in [exp.CTE, exp.Subquery, exp.Values]:
                cols = {}
                for n, c in zip(table.named_selects, table.selects):
                    if n == '*':
                        sc = self.trace(c)
                        cols.update(sc)
                    else:
                        cols[n] = self.trace(c) 
                type = {
                    exp.CTE: 'cte',
                    exp.Subquery: 'sq',
                    exp.Values: 'values'
                }[_type(table)]
                ts = mdl.TableSource(
                    name+'.'+type,
                    cols,
                    type=type
                )
                return ts
            else:
                tbl = self.resolve_table(table)
                if tbl:
                    return tbl
                else:
                    raise ValueError(f'unable to resolve columns for table {table}')
        
         
        def trace_cte_column(self, cte, col_name):
            if col_name in cte.named_selects:
                return self.trace(cte.selects[cte.named_selects.index(col_name)])
            elif '*' in cte.named_selects:
                sc = self.get_star_cols(cte.args['this'])
                return sc.get(col_name, None)
            
        
        
         
        def get_star_cols(self, select):
            ts = self.trace_table_structure(select)
            columns = {c: mdl.ColumnSource(ts, c) for c in ts.columns}
            return columns
                
         
        def trace_window(self, w):
            if type(w.this) == exp.RowNumber:
                return mdl.ConstantSource('ROW_NUMBER')
            elif w.this.alias_or_name == 'DENSE_RANK':
                return mdl.ConstantSource(w.this.alias_or_name)
            return mdl.UnknownSource('[Window] '+str(w))
        
         
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
                if type(r) == mdl.TransformSource and r.transform == 'CONCAT':
                    sources = r.sources + [et]
                else:
                    sources = [r, et]
                r = mdl.TransformSource('CONCAT', sources)
            return r
         
        def trace_function_call(self, d):
            name = d.name or d.key.upper()
            params = list(d.expressions) if d.expressions else [d.args['this']]
            
            if _type(d) == exp.Substring:
                args = str(d.args['start'])
                if 'length' in d.args:
                    args = args+','+str(d.args['length'])
                return mdl.TransformSource(f'SUBSTRING({args})', self.trace(d.args['this']))
            elif _type(d) == exp.SplitPart:
                args = str(d.args['delimiter'])+','+str(d.args['part_index'])
                return mdl.TransformSource(f'SPLIT_PART({args})', self.trace(d.args['this']))
            elif _type(d) == exp.TimeToStr:
                args = str(d.args['delimiter'])+','+str(d.args['part_index'])
                return mdl.TransformSource(f'TIME_TO_STR({args})', self.trace(d.args['this']))
            elif _type(d) == exp.JSONExtract:
                r = self.trace_column(d.args['this'])
                path = str(d.args['expression'])
                path = path.strip("'")
                if path.startswith('$.'):
                    path = path[2:]
                return mdl.PathSource(path, r)
            elif len(params) == 1:
                return mdl.TransformSource(name, self.trace(params[0]))
            
            return mdl.UnknownSource(f'[FunctionCall]: {d}')
        
         
        def trace_coalesce(self, coalesce):
            return mdl.CompositeSource(
                [
                    self.trace(x) 
                    for x in [
                        coalesce.args['this']
                    ]+coalesce.args['expressions']
                ]
            )
            
        
        def trace_struct(self, struct):
            return mdl.StructSource(
                {
                    e.name: self.trace(e.expression) 
                    for e in struct.expressions
                }
            )

            
        def trace_conditional(self, e):
            if _type(e) == exp.If:
                raise ValueError('not implemented')
            elif _type(e) == exp.Case:
                src = self.trace(e.args.get('default'))
                for iff in reversed(e.args['ifs']):
                    src = mdl.ConditionalSource(
                        self.trace(iff.args['this']), 
                        self.trace(iff.args['true']), 
                        src
                    )
                return src
            else:
                raise ValueError(f'unhandled: {e}')
        
        def trace(self, e):
            tracer = self.tracers.get(type(e))
            if tracer:
                return tracer(self, e)
            else:
                return self._trace(e)
         
        def _trace(self, e):
            if type(e) == exp.Identifier:
                return self.trace(e.parent)
            elif type(e) == exp.Column:
                return self.trace_column(e)
            elif type(e) in [exp.Alias, exp.Cast, exp.Paren]:
                return self.trace(e.args['this'])
            elif type(e) == exp.Window:
                return self.trace_window(e)
            elif type(e) == exp.Coalesce:
                return self.trace_coalesce(e)
            elif type(e) == exp.Struct:
                return self.trace_struct(e)
            elif type(e) == exp.Case:
                return self.trace_conditional(e)
            elif type(e) == exp.DPipe:
                return self.trace_dpipe(e)
            elif type(e) == exp.Array:
                return mdl.TransformSource('ARRAY', [self.trace(ex) for ex in e.expressions])
            elif type(e) == exp.Dot:
                return self.trace(e.args['expression'])
            elif type(e) in [exp.Trim, exp.Max, exp.Upper, exp.Substring, exp.SplitPart, exp.TimeToStr, exp.JSONExtract]:
                return self.trace_function_call(e)
            elif type(e) == exp.Star:
                return self.get_star_cols(e.parent_select)
            elif type(e) == exp.Bracket:
                col_src = self.trace(e.args['this'])
                if type(col_src) == mdl.ColumnSource:
                    return mdl.TransformSource(f'LIST_INDEX[{e.output_name}]', col_src)
                return col_src
            elif type(e) in [exp.Literal, exp.CurrentDate, exp.Boolean]:
                return mdl.ConstantSource(str(e))
            elif type(e) == exp.Count:
                return mdl.ConstantSource('COUNT')
            elif type(e) == exp.Null:
                return mdl.ConstantSource('NULL')
            elif isinstance(e, mdl.Source):
                raise ValueError('something is wrong')
            elif type(e) == exp.Lower:
                return mdl.TransformSource('LOWER', self.trace(e.args['this']))
            elif type(e) == exp.Not:
                return mdl.TransformSource('NOT', self.trace(e.args['this']))
            elif type(e) == exp.Explode:
                if _type(e.args['this']) != exp.Array:
                    raise ValueError(f'Unhandled explode: {e.args["this"]}')
                return mdl.UnionSource(sources=[self.trace(se) for se in e.args['this'].args['expressions']])
            elif type(e) == exp.Is and type(e.right) == exp.Null:
                return mdl.TransformSource('IS NULL', [self.trace(e.left)])
            elif type(e) in [exp.Add, exp.Is]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(),
                    [
                        self.trace(e.left), 
                        self.trace(e.right)
                    ]
                )
            elif type(e) in [exp.EQ, exp.GT, exp.LT, exp.Is, exp.NullSafeNEQ, exp.NullSafeEQ]:
                return mdl.ComparisonSource(
                    e.__class__.__name__.upper(), 
                    self.trace(e.left), 
                    self.trace(e.right)
                )
            else:
                return mdl.UnknownSource(f'[{type(e).__name__}] {e}')