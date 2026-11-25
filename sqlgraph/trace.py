from sqlgraph.graph import SqlGraph
from sqlglot import parse_one, exp
import os
import logging
import json
from sqlgraph import model as mdl
from uuid import uuid4
from sqlgraph.model import CompositeSource

_type = type


def _arg(expression, name):
    """
    Reads an arg that sqlglot 28 renamed to avoid clashing with Python keywords ('from' -> 'from_', 'with' -> 'with_').
    Works on either side of the rename.
    """
    for key in (f'{name}_', name):
        value = expression.args.get(key)
        if value is not None:
            return value
    return None


def _safe_sql(e):
    """
    Renders a node for an UNKNOWN label.  sqlglot's generator can raise on nodes it half-supports (e.g. CHR(..) in
    28.10 - 30.10); a label that can't be rendered must not fail the whole trace.
    """
    try:
        return str(e)
    except Exception:
        return f'<{type(e).__name__}>'


def _literal_text(e):
    """
    How a function argument is shown in a label.  E'..' / raw strings print as an empty string through sqlglot's
    default generator in newer releases, so read their text directly; everything else is rendered as SQL as before.
    """
    if _type(e) in [exp.ByteString, exp.RawString]:
        return e.name
    return str(e)


# Functions that older sqlglot parses as a generic `Anonymous` call (traced from their arguments, see _trace) but that
# newer releases promote to a typed node.  They are traced exactly as the Anonymous call was, so a sqlglot upgrade
# doesn't change their lineage.  Names missing from the installed sqlglot are skipped.
_FORMERLY_ANONYMOUS = tuple(
    cls for cls in (
        getattr(exp, name, None) for name in (
            'Replace', 'ArrayRemove', 'Ascii', 'DenseRank', 'ArrayPosition', 'Trunc', 'Variadic', 'JSONBObjectAgg',
        )
    ) if cls is not None
)

# sqlglot's sql_name() mangles acronym-prefixed classes (JSONObjectAgg -> J_S_O_N_OBJECT_AGG); use the SQL names
_TYPED_FUNCTION_NAMES = {
    'JSONObjectAgg': 'JSON_OBJECT_AGG',
    'JSONBObjectAgg': 'JSONB_OBJECT_AGG',
}

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
            self.traced_tables = {}
            self.dialect = dialect
            self.parsing_context = []

            # NOTE: sqlglot expressions compare (and hash) *structurally*, so two different nodes with identical SQL
            # - e.g. the whole statement of two different models - are "equal".  Both of these are therefore keyed by
            # object identity, and keep a reference to the expression so its id() can't be reused by another object.
            self.unique_names = {} # id(expression) -> (expression, name)
            self.node_cache = {} # id(expression) -> (expression, traced table structure)
            
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
                    # chain the original exception (and include its text) so the real cause - e.g. a parse error
                    # or an unexpected AST shape after a sqlglot upgrade - isn't hidden behind this generic message
                    raise ValueError(f'Error parsing sql for {table_id}: {type(ex).__name__}: {ex}') from ex
            return self.traced_tables
            
            
        def get_unique_name(self, e):
            if id(e) not in self.unique_names:
                ctx = self.parsing_context[-1]
                name = f'{ctx["table_id"]}_{ctx["unique_idx"]}'
                ctx['unique_idx'] = ctx['unique_idx'] + 1
                self.unique_names[id(e)] = (e, name)
            return self.unique_names[id(e)][1]

        def get_comments(self, tbl):
            comments = tbl.comments or list()
            with_ = _arg(tbl, 'with')
            if with_ is not None:
                comments.extend(with_.comments or list())
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
           
        
        
        def get_visible_sources(self, column):
            # sources a column can resolve against. A column inside a FROM/JOIN source expression
            # (e.g. an argument of a table function) can't refer to that source's own output, and
            # trying to would re-trace the source forever; ON/WHERE/SELECT columns are unaffected.
            ancestors = []
            node = column.parent
            while node is not None:
                ancestors.append(node)
                node = node.parent
            return {
                n: s for n, s in self.get_select_sources(column.parent_select).items()
                if _type(s) not in [exp.From, exp.Join] or not any(a is s.args.get('this') for a in ancestors)
            }

        def get_enclosing_lateral(self, select):
            # returns the nearest Lateral this select is nested inside, at any depth, if any.  Anything inside a
            # LATERAL's body (including derived tables nested in it) can refer to the sources to the lateral's left.
            parent = select.parent
            while parent is not None and _type(parent) != exp.Lateral:
                parent = parent.parent
            return parent

        def get_select_sources(self, select, include_outer=True):
            sources = {}
            from_ = _arg(select, 'from')
            if from_ is not None:
                own = [from_]
                own.extend(select.args.get('joins', []))
                sources = {s.alias_or_name or f'src_{i}': s for i, s in enumerate(own)}

            # a select inside a LATERAL can reference sources that appear earlier in the enclosing
            # select (correlated references); make those visible, with the select's own sources winning
            lateral = self.get_enclosing_lateral(select) if include_outer else None
            if not sources and lateral is None:
                raise ValueError(f'unhandled select sources: {select}')

            if lateral is not None:
                parent_select = lateral.parent_select
                parent_own = self.get_select_sources(parent_select, include_outer=False)

                # sources to the left of the lateral in its parent select...
                for pn, ps in parent_own.items():
                    if ps.args.get('this') is lateral:
                        break
                    sources.setdefault(pn, ps)

                # ...plus anything the parent select can itself see from enclosing laterals
                for pn, ps in self.get_select_sources(parent_select).items():
                    if pn not in parent_own:
                        sources.setdefault(pn, ps)

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
                
            if id(t) not in self.node_cache:
                self.node_cache[id(t)] = (t, self._do_trace_table_structure(t, name, type=type, select_sources=select_sources))

            return self.node_cache[id(t)][1]
        
        def _do_trace_table_structure(self, t, name, *, type=None, select_sources=None):
            print(f'Tracing [{t.type}]: {name}')
                
            if _type(t) in [exp.Lateral]:
                if _type(t.args['this']) in [exp.Subquery]:
                    sq_ts = self.trace_table_structure(
                        t.args['this'],
                        name=f'{name}.{t.__class__.__name__.lower()}',
                        select_sources=select_sources
                    )

                    # LATERAL (...) AS a(j, slot): the alias column list renames the subquery's output
                    # columns by position, so expose them under those names, linked to the subquery's
                    # columns (columns beyond the alias list keep their own names)
                    alias = t.args.get('alias')
                    alias_cols = [c.name for c in alias.args.get('columns') or []] if alias else []
                    if alias_cols:
                        orig_cols = list(sq_ts.columns)
                        renamed = {}
                        for i, orig in enumerate(orig_cols):
                            renamed[alias_cols[i] if i < len(alias_cols) else orig] = mdl.ColumnSource(sq_ts, orig)
                        return mdl.TableSource(name, renamed, type=sq_ts.type)
                    return sq_ts
                else:
                    return self.get_columns_for_table(t, name)
            elif _type(t) in [exp.From, exp.Join]:
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
                        # own sources only: a star must not expand correlated outer (lateral) sources
                        select_sources = self.get_select_sources(t, include_outer=False)
                    for src in select_sources.values():
                        trc = self.trace_table_structure(src, name=name+'.src')
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
                is_recursive = self.is_recursive(t, name)
                original_t = t
                recursive_table = None
                if is_recursive:
                    # the final union select is the recursive select
                    recursive_table = t.right
                    # all others are anchor selects
                    t = t.left 
                    
                
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
                
                if recursive_table:
				    # if we have a recursive CTE, we have to do some manual manipulation to prevent circular references
					# 1. store the parsed "anchor" tables (ts) in the cache so that the recursive portion can reference them
                    self.node_cache[id(original_t)] = (original_t, ts)
					# 2. parse the recursive table portion (which will reference the anchor tables)
                    rts = self.trace_table_structure(recursive_table, name=name+f'.union_recursive', select_sources=select_sources)
                    
					# 3. Replace any references to the anchor tables in the recursive tables with references to the 
					#    transitive source.  This will prevent self-referential loops
                    mdl.RecursiveReplacement.replace_table(rts, ts)
                        
					# 4. Now combine the sources of the anchor clauses with the sources of the recursive clause into one
					#    note that we use a temporary dictionary in case there are multiple replacements of the same column    
                    new_union_sources = {
                        column: mdl.UnionSource(
                            left=ts.sources[column], 
                            right=rts.sources.get(column, [])
                        )
                        for column in ts.columns
                    }
                    ts.sources = new_union_sources
   
   					# 5. Finally, returned the modified source
                    return ts
                
                return ts
            else:
                return self.get_columns_for_table(t, name)
        
        def is_recursive(self, union_table, name):
            
            # recursive query must be union of one or more anchor queries and a single recursive query
            try:
                last_union_table = union_table.right
                last_union_src = self.get_table(last_union_table, _arg(last_union_table, 'from'))
                if last_union_src != last_union_table.parent:
                    return False
            except Exception as ex:
                print(ex)
                return False
            
            #at this point, we know it's recursive
            print(f'processing recursive CTE: {name}')
            return True
         
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
                select_sources = self.get_visible_sources(column)
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

                for s in select_sources.values():
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
                for s in self.get_visible_sources(column).values():
                    ts = self.trace_table_structure(s)
                    if column.name in ts.columns:
                        return s.args['this']
        
         
        def get_columns_for_table(self, table, name):
            if _type(table) in [exp.CTE, exp.Subquery, exp.Values, exp.Select]:
                
                type = {
                    exp.CTE: 'cte',
                    exp.Subquery: 'sq',
                    exp.Values: 'values',
                    exp.Select: 'select'
                }[_type(table)]
                
                tbl_src = self.trace_table_structure(table, name=name)
                return mdl.TableSource(
                    tbl_src.name,
                    tbl_src.sources,
                    type=type
                )

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
                table_alias = table.args.get('alias')
                if table_alias is not None and table_alias.args.get('columns'):
                    col_names = [c.name for c in table_alias.args['columns']]
                elif table_alias is not None:
                    col_names = [table_alias.args['this'].name]
                else:
                    # no alias (e.g. FROM JSON_ARRAY_ELEMENTS(x) WITH ORDINALITY): the output column is
                    # named after the function, plus an 'ordinality' column when requested
                    fn = table.args.get('this', table)
                    fn_name = (getattr(fn, 'name', None) or fn.sql_name()).lower()
                    col_names = [fn_name]
                    if table.args.get('ordinality'):
                        col_names.append('ordinality')

                # WITH ORDINALITY appends a row-number column, which doesn't derive from the function's input
                ordinality_col = col_names[-1] if table.args.get('ordinality') and len(col_names) > 1 else None
                return mdl.TableSource(
                    name,
                    {n: (mdl.ConstantSource(value='ORDINALITY') if n == ordinality_col else func_src) for n in col_names},
                    'table_function'
                )
        
         
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
            # Postgres binds `::` tighter than `->>`, so `j->>'key'::text` casts the key, not the result.  Newer sqlglot
            # parses it that way and hands us Cast(Literal) instead of a JSONPath: unwrap it back to the plain key.
            while _type(jp) == exp.Cast:
                jp = jp.args['this']
            if _type(jp) == exp.Literal:
                if jp.is_string:
                    return f'"{jp.name}"' if ' ' in jp.name else jp.name
                return f'[{jp.name}]'
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
                args = _literal_text(d.args['delimiter'])+','+str(d.args['part_index'])
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
                    f'REGEXP_REPLACE({_literal_text(d.args["expression"])}, {_literal_text(d.args["replacement"])})',
                    self.trace(d.args['this'])
                )
            elif len(params) == 1:
                return mdl.TransformSource(name, self.trace(params[0]))
            
            return mdl.UnknownSource(f'[FunctionCall]: {d}')
            
        def trace_generic_composite(self, e):
            source_elements = []
            if 'this' in e.args and e.args['this']:
                source_elements.append(e.args['this'])
            
            if 'expression' in e.args and e.args['expression']:
                source_elements.append(e.args['expression'])
                
            if 'expressions' in e.args and e.args['expressions']:
                source_elements.extend(e.args['expressions'])

            return mdl.CompositeSource(
                [
                    self.trace(x) 
                    for x in source_elements
                ],
                name=e.__class__.__name__.upper()
            )
            
        def trace_generic_transform(self, e):
            source_elements = []
            if 'this' in e.args and e.args['this']:
                source_elements.append(e.args['this'])
            
            if 'expression' in e.args and e.args['expression']:
                source_elements.append(e.args['expression'])
                
            if 'expressions' in e.args and e.args['expressions']:
                source_elements.extend(e.args['expressions'])
            
            return mdl.TransformSource(
                transform=e.__class__.__name__.upper(),
                sources=[
                    self.trace(x) 
                    for x in source_elements
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
        
        def trace_is(self, e):
            if type(e.right) == exp.Null:
                return mdl.TransformSource('IS NULL', [self.trace(e.left)])
            return mdl.TransformSource('IS', {'left': self.trace(e.left), 'right': self.trace(e.right)})

        def trace_typed_function(self, e):
            # same as the Anonymous branch of _trace: the output derives from the call's arguments, in order
            fn_name = _TYPED_FUNCTION_NAMES.get(type(e).__name__) or (
                e.sql_name() if isinstance(e, exp.Func) else type(e).__name__.upper()
            )
            args = []
            for key in e.arg_types:
                value = e.args.get(key)
                if isinstance(value, exp.Expression):
                    args.append(value)
                elif isinstance(value, list):
                    args.extend(v for v in value if isinstance(v, exp.Expression))
            if not args:
                return mdl.ConstantSource(value=fn_name)
            return mdl.TransformSource(fn_name, [self.trace(x) for x in args])

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
                             exp.Length, exp.Sum, exp.FirstValue]:
                return self.trace(e.args['this'])
            elif type(e) == exp.Window:
                return self.trace_window(e)
            elif type(e) in [exp.Least, exp.Greatest, exp.Coalesce, exp.Tuple, exp.Any]:
                return self.trace_generic_composite(e)
            elif type(e) in [exp.Pad, exp.Filter, exp.Left]:
                return self.trace_generic_transform(e)
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
                # SPECIAL CASE FOR JSONB_EXTRACT_SCALAR (TODO: may need to handle json version here as well)
                #the parser produces a strange output for dynamic paths.  If we happen to get one of those,
                #convert it to the same format as static path
                if type(e.this) == exp.JSONBExtractScalar and e.this.expression.alias_or_name == 'ARRAY':
                    e.this.args['expression'] = e.expressions[0]
                
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
            elif type(e) in [exp.Like, exp.ILike] and e.args.get('negate'):
                # newer sqlglot parses `a NOT LIKE b` as Like(negate=True) instead of Not(Like(..)): same lineage as before
                return mdl.TransformSource(
                    'NOT',
                    mdl.TransformSource(
                        e.__class__.__name__.upper(),
                        {'left': self.trace(e.left), 'right': self.trace(e.right)}
                    )
                )
            elif type(e) == exp.Is and e.args.get('negate'):
                # newer sqlglot parses `a IS NOT NULL` as Is(negate=True) instead of Not(Is(..)): same lineage as before
                return mdl.TransformSource('NOT', self.trace_is(e))
            elif type(e) == exp.Is and type(e.right) == exp.Null:
                return self.trace_is(e)
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
                             exp.And, exp.Or, exp.Mul, exp.LTE, exp.GTE, exp.SimilarTo]:
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
                first = e.expressions[0] if e.expressions else None
                if first is not None and isinstance(first, getattr(exp, 'JSONKeyValue', ())):
                    # JSON_OBJECTAGG(k : v): the argument is a single key/value pair node
                    return mdl.TransformSource(
                        e.__class__.__name__.upper(),
                        [
                            self.trace(first.args['this']),
                            self.trace(first.args['expression'])
                        ])
                # json_object_agg(k, v): newer sqlglot types this call, with the arguments as plain expressions
                return self.trace_typed_function(e)
            elif _FORMERLY_ANONYMOUS and isinstance(e, _FORMERLY_ANONYMOUS):
                return self.trace_typed_function(e)
            elif type(e) in [exp.StrPosition]:
                return mdl.TransformSource(
                    e.__class__.__name__.upper(), 
                    [
                        self.trace(e.args['this']),
                        self.trace(e.args['substr'])
                    ])
            elif type(e) in [exp.ExplodingGenerateSeries]:
                return mdl.ConstantSource(value=f'SERIES[{e.args["start"]}..{e.args["end"]}]')
            elif type(e) in [exp.ByteString, exp.RawString]:
                return mdl.ConstantSource(value=e.args['this'])
            elif type(e) in [exp.Lag, exp.Lead]:
                return mdl.CompositeSource(
                    sources=[self.trace(e.args['this'])],
                    name=e.__class__.__name__.upper()
                )
            elif type(e) in [exp.Encode]:
                return mdl.TransformSource(
                    f'{e.__class__.__name__.upper()}({e.args["charset"]})', 
                    [
                        self.trace(e.args['this'])
                    ]
                )
            elif type(e) in [exp.DataType]:
                return mdl.ConstantSource(value=f'DATATYPE[{e.sql()}]')
            elif type(e) == exp.Anonymous:
                # generic function call (e.g. JSON_BUILD_ARRAY, JSON_ARRAY_ELEMENTS): its output derives from its arguments
                fn_name = str(e.name).upper()
                if not e.expressions:
                    return mdl.ConstantSource(value=fn_name)
                return mdl.TransformSource(fn_name, [self.trace(x) for x in e.expressions])
            else:
                return mdl.UnknownSource(f'[{type(e).__name__}] {_safe_sql(e)}')