from sqlgraph.graph import SqlGraph
from sqlglot import parse_one, exp
import os
import logging
import json
from sqlgraph import model as mdl
from uuid import uuid4
from sqlgraph.model import CompositeSource

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
        for table, table_source in self.tables.items():
            if s:
                s += '\n\n'
            s += table + '\n'
            s += '------------------\n'
            for column, source in table_source.sources.items():
                s += f'  {column}: {source}\n'
        return s

        
    @classmethod
    def trace_sql(cls, sql, name=None, *, models=None, excluded_models=None, dialect=None, schema=None, db=None, catalog=None, tracers=None):
        if type(sql) == str:
            if not name:
                raise ValueError('name is required for single SQL statement')
            else:
                sql = {name: sql}
                
        filtered = {}
        for model, sql in sql.items():
            if models is not None:
                if model not in models:
                    continue
            elif excluded_models and model in excluded_models:
                continue
            filtered[model] = sql
        sql = filtered
                
        # qualify table names if passed
        if db or catalog:
            sql = {
                mdl.Table.get_id(name, db, catalog): s
                for name, s in sql.items()
            }
                
        tables = cls.Tracer(sql, dialect=dialect, schema=schema, tracers=tracers).trace_sql()
        return SqlTrace(tables)
        
    @classmethod
    def trace_file(cls, file, *, name=None, **kwargs):
        if not name:
            name = os.path.basename(file).rsplit('.', 1)[0]
        with open(file) as f:
            sql = f.read()
        return cls.trace_sql({name: sql}, **kwargs)
    
    @classmethod
    def trace_directory(cls, directory, **kwargs):
        sqls = {}
        for root, dirs, files in os.walk(directory):
            for file in files:
                model = file[0:-4]
                
                with open(os.path.join(root, file)) as f:
                    sql = f.read()
                sqls[model] = sql
                    
        return cls.trace_sql(sqls, **kwargs)
    
    @classmethod
    def list_models(cls, directory):
        models = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                models.append(file[0:-4])
        return models
    
    class Tracer():
        
        def __init__(self, sqls, *, dialect=None, schema=None, tracers=None):
            self.sqls = sqls
            self.schema = schema
            self.column_cache = {}
            self.tracers = tracers or {}
            self.unique_names = []
            self.traced_tables = {}
            self.dialect = dialect
            self.parsing_context = []
            
        def get_traced_table(self, table_id):
            if table_id not in self.sqls:
                return None
            
            if table_id not in self.traced_tables:
                self.parsing_context.append({'table_id': table_id, 'unique_idx': 0})
                s = self.sqls[table_id]
                t = parse_one(s, dialect=self.dialect)
                qualified_table = mdl.Table.from_id(table_id)
                tbl = self.trace_table(t, qualified_table.name)
                tbl.db = qualified_table.db
                tbl.catalog = qualified_table.catalog
                self.traced_tables[table_id] = tbl
                self.parsing_context.pop()
            return self.traced_tables[table_id]
            
            
        def trace_sql(self):
            for table_id in self.sqls.keys():
                try:
                    self.get_traced_table(table_id)
                except Exception as ex:
                    raise ValueError(f'Error parsing sql for {table_id}')
            return self.traced_tables
            
            
        def get_unique_name(self, e):
            name = None
            for k, v in self.unique_names:
                if k == e:
                    name = v
                    break
                
            if not name:
                ctx = self.parsing_context[-1]
                name = f'{ctx["table_id"]}_{ctx["unique_idx"]}'
                ctx['unique_idx'] = ctx['unique_idx'] + 1
                self.unique_names.append([e, name])
            return name

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
                tbl = self.get_traced_table(mdl.Table(t.name, [], db=t.db or None, catalog=t.catalog or None).id)
                if not tbl and self.schema:
                    tbl = self.schema.get_table(t.name, t.db or None, t.catalog or None)
                
                self.column_cache[table_key] = tbl

            return self.column_cache[table_key]
        
         
        def find_direct(self, parent, exp_type):
            return list([x for x in parent.find_all(exp_type) if x.find_ancestor(parent.__class__) == parent])
        
         
        def get_applicable_ctes(self, select): 
            ctes = [] 
            cte_select = select
            while True:
                if hasattr(cte_select, 'ctes') and cte_select.ctes:
                    ctes.extend(cte_select.ctes)
                
                if cte_select.parent and not cte_select.same_parent:
                    cte_select = cte_select.parent
                elif cte_select.parent_select:
                    cte_select = cte_select.parent_select
                else:
                    break
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
            if name is None:
                name = self.get_unique_name(t)
                
            if _type(t) in [exp.Lateral]:
                if _type(t.args['this']) in [exp.Subquery]:
                    return self.trace_table_structure(
                        t.args['this'], 
                        name=f'{name}.{t.__class__.__name__.lower()}',
                        select_sources=select_sources
                    )
                else:
                    return self.get_columns_for_table(t, name)
            elif _type(t) in [exp.From, exp.Join, exp.Lateral]:
                return self.trace_table_structure(
                    t.args['this'], 
                    name=f'{name}.{t.__class__.__name__.lower()}',
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
                    name=f'{name}.{tag}',
                    select_sources=select_sources
                )
            elif _type(t) in [exp.Select]:
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
            elif _type(t) in [exp.Values]:
                ts = mdl.TableSource(name, {c.alias_or_name: mdl.ConstantSource(value='VALUES') for c in t.args['alias'].columns}, type='values')
                return ts
            elif _type(t) in [exp.Table]:
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
                return self.get_columns_for_table(t, name)
        
         
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
                select_sources = self.get_select_sources(column.parent_select)
                traced_ss = [] # for debugging
                for s in select_sources.values():
                    try:
                        ts = self.trace_table_structure(s)
                        if column.name in ts.columns:
                            return mdl.ColumnSource(ts, column.name)
                        else:
                            traced_ss.append(ts) # for debugging
                    except Exception as ex:
                        print(ex)
                ##debugging
                print('debugging')
                for s in self.get_select_sources(column.parent_select).values():
                    try:
                        ts = self.trace_table_structure(s)
                        if column.name in ts.columns:
                            return mdl.ColumnSource(ts, column.name)
                    except:
                        pass
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
            elif _type(table) == exp.Table and _type(table.args['this']) in [exp.Identifier]:
                # actual table
                tbl = self.resolve_table(table)
                if tbl:
                    return tbl
                else:
                    raise ValueError(f'unable to resolve columns for table {table}')
            else:
                # table function
                if 'this' in table.args:
                    trace_val = table.args['this']
                elif _type(table) in [exp.Unnest]:
                    trace_val = table.args['expressions'][0]
                else:
                    raise ValueError(f'unsupported table function {table}')
                
                func_src = self.trace(trace_val)
                if 'alias' not in table.args:
                    print('pause')
                    
                table_alias = table.args['alias']
                if table_alias.args.get('columns'):
                    col_names = [c.name for c in table_alias.args['columns']]
                else:
                    col_names = [table_alias.args['this'].name]

                return mdl.TableSource(name, {n: func_src for n in col_names}, 'table_function')
        
         
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
            else:
                return self.trace(w.args['this'])
        
         
        def get_src_columns(self, t):
            if 'src_columns' in t:
                return t['src_columns']
            elif 'src_column' in t:
                return [t['src_column']]
            else:
                return []
        
         
        def trace_dpipe(self, p):
            
            sources = []
            left = self.trace(p.left)
            if _type(left) == mdl.TransformSource and left.name == 'CONCAT':
                sources.extend(left.sources)
            else:
                sources.append(left)
                
            right = self.trace(p.right)
            if _type(right) == mdl.TransformSource and right.name == 'CONCAT':
                sources.extend(right.sources)
            else:
                sources.append(right)
            
            return mdl.TransformSource('CONCAT', sources)

        def json_path_to_str(self, jp):
            s = []
            for e in jp.expressions:
                if _type(e) == exp.JSONPathRoot:
                    s.append('$')
                elif _type(e) == exp.JSONPathKey:
                    p = e.alias_or_name
                    if ' ' in p:
                        p = f'"{p}"' 
                    s.append(p)
                elif _type(e) == exp.JSONPathSubscript:
                    p = e.alias_or_name
                    if ' ' in p:
                        p = f'"{p}"' 
                    s.append(f'[{e.args["this"]}]')
                else:
                    raise ValueError(f'unsupported path type: {e}')
            return '.'.join(s)
         
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
            elif _type(d) in [exp.TimeToStr, exp.StrToTime]:
                if 'delimiter' in d.args:
                    raise ValueError('check this')
                args = [str(d.args['format'])]
                return mdl.TransformSource(f'{d.__class__.__name__.upper()}({", ".join(args)})', self.trace(d.args['this']))
            elif _type(d) in [exp.JSONExtract, exp.JSONExtractScalar]:
                r = self.trace(d.args['this'])
                path = self.json_path_to_str(d.args['expression'])
                path = path.strip("'")
                if path.startswith('$.'):
                    path = path[2:]
                if _type(r) == mdl.PathSource:
                    #combine
                    r.path += '.'+path
                    return r
                else:
                    return mdl.PathSource(path, r)
            elif _type(d) in [exp.JSONBExtractScalar]:
                r = self.trace(d.args['this'])
                src = self.trace(d.args['expression'])
                if _type(src) == mdl.ConstantSource:
                    path = src.value.strip("'").strip('{}')
                    return mdl.PathSource(path, r)
                elif _type(src) in [mdl.CompositeSource, mdl.TransformSource]:
                    any_refs = any([_type(ss) != mdl.ConstantSource for ss in src.sources])
                    if any_refs:
                        return mdl.CompositeSource(
                            {
                                'json': r,
                                'path': src
                            },
                            name='JSON_EXTRACT_PATH'
                        )
                    else:
                        path = '.'.join([ss.value.strip("'").strip('{}') for ss in src.sources])
                        return mdl.PathSource(path, r)
                else:
                    return CompositeSource([r, src])
            elif type(d) == exp.RegexpReplace:
                return mdl.TransformSource(
                    f'REGEXP_REPLACE({d.args["expression"]}, {d.args["replacement"]})',
                    self.trace(d.args['this'])
                )
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
                ],
                name='COALESCE'
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
                default = e.args.get('default')
                if default is None:
                    default = exp.Null()
                src = self.trace(default)
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
            elif type(e) in [exp.Alias, exp.Cast, exp.Paren, exp.Max, exp.Min, exp.ArraySize, 
                             exp.Order, exp.JSONArrayAgg, exp.ArrayAgg, exp.ArrayToString, 
                             exp.StringToArray, exp.AnyValue, exp.Neg, exp.Where, exp.Initcap,
                             exp.Length, exp.Sum]:
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
            elif type(e) in [exp.Array, exp.Concat, exp.Distinct, exp.Unnest]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(ex) 
                        for ex in e.expressions
                    ]
                )
            elif type(e) in [exp.Dot, exp.Extract, exp.Kwarg]:
                return self.trace(e.args['expression'])
            elif type(e) in [exp.Trim, exp.Upper, exp.Substring, exp.SplitPart, exp.TimeToStr, exp.JSONExtract, 
                             exp.JSONExtractScalar, exp.JSONBExtractScalar, exp.StrToTime, exp.RegexpReplace,
                             exp.StrToDate]:
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
            elif type(e) in [exp.Count, exp.Null, exp.CurrentDate, exp.CurrentTimestamp, exp.Uuid]:
                return mdl.ConstantSource(e.__class__.__name__.upper())
            elif isinstance(e, mdl.Source):
                raise ValueError('something is wrong')
            elif type(e) in [exp.Lower, exp.Not, exp.UnixToTime, exp.GroupConcat, exp.Explode]:
                return mdl.TransformSource(e.__class__.__name__.upper(), self.trace(e.args['this']))
            elif type(e) == exp.Is and type(e.right) == exp.Null:
                return mdl.TransformSource('IS NULL', [self.trace(e.left)])
            elif type(e) == exp.In:
                return mdl.TransformSource(
                    'IN',
                    {
                        'left': self.trace(e.args['this']),
                        'right': mdl.TransformSource(
                            'SET',
                            [self.trace(ex) for ex in e.expressions]            
                        )
                    }
                )
            elif type(e) == exp.Between:
                return mdl.TransformSource(
                    'BETWEEN',
                    {
                        'value': self.trace(e.args['this']),
                        'low': self.trace(e.args['low']),
                        'high': self.trace(e.args['high'])
                    }
                )
            elif type(e) in [exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.Is, exp.NullSafeNEQ, exp.NullSafeEQ, 
                             exp.RegexpLike, exp.Like, exp.ILike, exp.Div, exp.Sub, exp.Add, exp.Is, 
                             exp.And, exp.Or, exp.Mul, exp.LTE, exp.GTE]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    {
                        'left': self.trace(e.left),
                        'right': self.trace(e.right)
                    }
                )
            elif type(e) in [exp.Subquery]:
                table_source = self.trace_table_structure(e)
                return mdl.CompositeSource(
                    sources=[
                        mdl.ColumnSource(table_source, column_name)
                        for column_name in table_source.columns
                    ]
                )
            elif type(e) in [exp.JSONObjectAgg]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(e.expressions[0].args['this']),
                        self.trace(e.expressions[0].args['expression'])
                    ])
            elif type(e) in [exp.Filter]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(e.args['this']),
                        self.trace(e.args['expression'])
                    ])
            elif type(e) in [exp.StrPosition]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(e.args['this']),
                        self.trace(e.args['substr'])
                    ])
            elif type(e) in [exp.ExplodingGenerateSeries]:
                return mdl.ConstantSource(value=f'SERIES[{e.args["start"]}..{e.args["end"]}]')
            elif type(e) == exp.ByteString:
                return mdl.ConstantSource(value=e.args['this'])
            elif type(e) == exp.Kwarg:
                print(e)
            else:
                return mdl.UnknownSource(f'[{type(e).__name__}] {e}')