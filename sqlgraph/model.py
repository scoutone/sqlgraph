import itertools
from types import NoneType


class Table():
    def __init__(self, name, columns, db=None, catalog=None, type='table'):
        self.name = name
        self.db = db
        self.catalog = catalog
        self.columns = columns
        self.type = type
        
    @staticmethod
    def get_id(name, db=None, catalog=None):
        s = name
        if db:
            s = db + '.' + s
            if catalog:
                s = catalog + '.' + s
        return s
       
    @property 
    def id(self):
        return Table.get_id(
            self.name, 
            self.db, 
            self.catalog
        )
        
    def __str__(self):
        return self.id
    
    def __repr__(self):
        return 'Table['+self.id+']'
    
    def __eq__(self, other):
        return isinstance(other, Table) and \
               self.name == other.name and \
               self.db == other.db and \
               self.catalog == other.catalog
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def matches(self, other):
        if self.name != other.name:
            return False
        
        if self.catalog != None and other.catalog != None and self.catalog != other.catalog:
            return False
        
        if self.db != None and other.db != None and self.db != other.db:
            return False
        return True
    
    def matches_id(self, other_id):
        return self.matches(Table.from_id(other_id))
    
    def to_dict(self):
        d = {
            'type': self.type,
            'name': self.name
        }
        
        if self.db:
            d['db'] = self.db
        if self.catalog:
            d['catalog'] = self.catalog
            
        d['id'] = self.id
        d['columns'] = self.columns
        return d
    
    @staticmethod
    def from_id(table_id, columns=None, type='table'):
        parts = table_id.split('.')
        name = parts[-1]
        db = parts[-2] if len(parts) > 1 else None
        catalog = parts[-3] if len(parts) > 2 else None
        return Table(name, columns, db=db, catalog=catalog, type=type)
    
    @staticmethod
    def ids_match(table_id1, table_id2):
        return Table.from_id(table_id1).matches_id(table_id2)
    
class Source():
    def __init__(self, *, notes=None, internal=None):
        self.notes = notes
        self.internal = internal

        
    def to_dict(self):
        d = {'type': 'source'}
        if self.notes:
            d['notes'] = self.notes
        if self.internal:
            d['internal'] = self.internal
        return d
    
    def as_list(self):
        return [self]
    
    def all_sources(self):
        return self.as_list()
    
    def __str__(self):
        return str(self.to_dict())
        
class CompositeSource(Source):
    def __init__(self, sources=[], *, name=None, **kwargs):
        Source.__init__(self, **kwargs)
        self.sources = sources
        self.name = name
                
    def as_list(self):
        return self.sources
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
        
    def to_dict(self):
        d = {
            'type': 'composite',
            'name': self.name
        }
        
        if type(self.sources) == list:
            d['sources'] = [x.to_dict() for x in self.sources]
        else:
            d['sources'] = {name: x.to_dict() for name, x in self.sources.items()}
        
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
    
class StructSource(Source):
    def __init__(self, sources, *args, **kwargs):
        self.sources = sources
        super().__init__(*args, **kwargs)
        
    def to_dict(self):
        d = {
            'type': 'struct',
            'sources': {
                name: value.to_dict()
                for name, value in self.sources.items()
            }
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
                
    def as_list(self):
        return list(self.sources.values()) if self.sources else []
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
    
class TableSource(Table):
    def __init__(self, name, sources, type, *, db=None, catalog=None):
        super().__init__(name, list(sources.keys()), db=db, catalog=catalog, type=type)
        self.sources = sources
        
    def to_dict(self):
        d = super().to_dict()
        d['sources'] = {
            c: s.to_dict()
            for c, s in self.sources.items()
        }
        return d
                
    def as_list(self):
        return list(self.sources.values()) if self.sources else []
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
    
class TransformSource(CompositeSource):
    def __init__(self, transform, sources, **kwargs):
        if sources and not type(sources) == list and not type(sources) == dict:
            sources = [sources]
            
        super().__init__(sources=sources, name=transform, **kwargs)
        
    def to_dict(self):
        d = {
            'type': 'transform'
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
                
    def as_list(self):
        if not self.sources:
            return []
        elif type(self.sources) == list:
            return self.sources
        else:
            return list(self.sources.values())
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))

class UnknownSource(Source):
    def __init__(self, msg, **kwargs):
        Source.__init__(self, **kwargs)
        self.msg = msg
        
    def to_dict(self):
        d = {
            'type': 'unknown',
            'UNKNOWN': self.msg
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d

class ConstantSource(Source):
    def __init__(self, value, **kwargs):
        Source.__init__(self, **kwargs)
        self.value = value
        
    def to_dict(self):
        d = {
            'type': 'constant',
            'constant': self.value,
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
        
class ColumnSource(Source):
    def __init__(self, table, column, **kwargs):
        Source.__init__(self, **kwargs)
        self.table = table
        self.column = column
        
    def to_dict(self):
        #print(f'table type: {type(self.table)} [{self.table}]')
        d = {
            'type': 'column',
            'table': self.table if type(self.table) == str else self.table.id,
            'column': self.column
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d

class ComparisonSource(Source):
    def __init__(self, name, left, right, **kwargs):
        Source.__init__(self, **kwargs)
        self.name = name
        self.left = left
        self.right = right
        
    def to_dict(self):
        d = {
            'type': 'comparison',
            'name': self.name,
            'left': self.left.to_dict(),
            'right': self.right.to_dict()
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
                
    def as_list(self):
        val = []
        if self.left:
            val.append(self.left)
        if self.right:
            val.append(self.right)
        return val
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
        
       
    
class ConditionalSource(Source):
    def __init__(self, condition, true_value, false_value=None, **kwargs):
        Source.__init__(self, **kwargs)
        self.condition = condition
        self.true_value = true_value
        self.false_value = false_value if false_value else ConstantSource('NULL')
        
    def to_dict(self):
        d = {
            'type': 'conditional',
            'condition': self.condition.to_dict(),
            'true_value': self.true_value.to_dict(),
            'false_value': self.false_value.to_dict()
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
                
    def as_list(self):
        val = []
        if self.true_value:
            val.append(self.true_value)
        if self.false_value:
            val.append(self.false_value)
        return val
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
    
        
class PathSource(Source):
    def __init__(self, path, source, **kwargs):
        Source.__init__(self, **kwargs)
        self.path = path
        self.source = source
        
    def to_dict(self):
        d = {
            'type': 'path',
            'name': self.path,
            'source': self.source.to_dict()
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
    
    def to_list(self):
        return [self.source] if self.source else []
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
    
class UnionSource(CompositeSource):
    def __init__(self, left=None, right=None, *, sources=[], **kwargs):
        srcs = sources or []
        if type(left) == UnionSource:
            srcs.extend(left.sources)
        elif left:
            srcs.append(left)
        
        if type(right) == UnionSource:
            srcs.extend(right.sources)
        elif right:
            srcs.append(right)
        
        super().__init__(sources=srcs, **kwargs)
                
    def as_list(self):
        return self.sources
    
    def all_sources(self):
        return list(itertools.chain.from_iterable([x.all_sources() for x in self.as_list()]))
        
    def to_dict(self):
        d = {
                'type': 'union'
            }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d


class RecursiveReplacement(): 
    """
    This logic could have been implemented as member methods on the various model classes,
    however since it's pretty hacky logic to get around handling of recursive CTEs in
    postgres, I decided to keep it separate and isolated rather than polluting the models
    """
    @classmethod
    def replace_table(cls, root_src, transitive_table):
        for transitive_column in transitive_table.columns:
            cls.replace(
                root_src, 
                ColumnSource(transitive_table.name, transitive_column), 
                transitive_table.sources[transitive_column]
            )
        
    @classmethod
    def replace(cls, root_src, src_to_replace, replacement):
        SOURCE_CLASSES = [
            Source, 
            CompositeSource, 
            StructSource, 
            TableSource, 
            TransformSource, 
            UnknownSource, 
            ConstantSource, 
            ColumnSource, 
            ComparisonSource,
            ConditionalSource, 
            PathSource, 
            UnionSource
        ]
        
        def _eq(s1, s2):
            if not s1 or not s2:
                return False
            elif type(s1) != type(s2):
                return False 
            else:
                return s1.to_dict() == s2.to_dict()    
            
        def _replace_list(l, s_t_r, r):
            return list([_replace(x, s_t_r, r) for x in l])
            
        def _replace_dict(d, s_t_r, r):
            return {
                k: _replace(v, s_t_r, r)
                for k,v in d.items()
            }
            
        def _replace_list_or_dict(l_or_d, s_t_r, r):
            if type(l_or_d) == list:
                return _replace_list(l_or_d, s_t_r, r)
            elif type(l_or_d) == dict:
                return _replace_dict(l_or_d, s_t_r, r)
            else:
                return l_or_d
        
        def _replace(s, s_t_r, r):
            if type(s) in [Source, UnknownSource, ConstantSource, ColumnSource]:
                if _eq(s, s_t_r):
                    print(f's: {s}')
                    print(f's_t_r: {s_t_r}')
                    print(f'r: {r}')
                    return r 
                else:
                    return s
            else:
                if type(s) in [CompositeSource, StructSource, TableSource, TransformSource, UnionSource]:
                    s.sources = _replace_list_or_dict(s.sources, s_t_r, r)
                elif type(s) == ComparisonSource:
                    s.left = _replace(s.left, s_t_r, r)
                    s.right = _replace(s.right, s_t_r, r)
                elif type(s) == ConditionalSource:
                    s.true_value == _replace(s.true_value, s_t_r, r)
                    s.false_value == _replace(s.false_value, s_t_r, r)
                elif type(s) == PathSource:
                    s.source = _replace(s.source, s_t_r, r)
                return s
        
        _replace(root_src, src_to_replace, replacement)