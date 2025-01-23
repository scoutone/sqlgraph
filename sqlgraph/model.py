

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
    
    def __str__(self):
        return str(self.to_dict())
        
class CompositeSource(Source):
    def __init__(self, sources=[], *, name=None, **kwargs):
        Source.__init__(self, **kwargs)
        self.sources = sources
        self.name = name
                
    def as_list(self):
        return self.sources
        
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
    
class UnionSource(CompositeSource):
    def __init__(self, left=None, right=None, *, sources=[], **kwargs):
        sources = sources
        if type(left) == UnionSource:
            sources.extend(left.sources)
        elif left:
            sources.append(left)
        
        if type(right) == UnionSource:
            sources.extend(right.sources)
        elif right:
            sources.append(right)
        
        super().__init__(sources=sources, **kwargs)
                
    def as_list(self):
        return self.sources
        
    def to_dict(self):
        d = {
                'type': 'union'
            }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d