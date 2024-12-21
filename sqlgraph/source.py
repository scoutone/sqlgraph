
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
    def __init__(self, sources=[], **kwargs):
        Source.__init__(self, **kwargs)
        self.sources = sources
                
    def as_list(self):
        return self.sources
        
    def to_dict(self):
        d = {
            'type': 'composite',
            'sources':  list([s.to_dict() for s in self.sources])
        }
        
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
    
class TableSource():
    def __init__(self, name, columns, type, *, db=None, catalog=None):
        self.name = name
        self.type = type
        self.columns = columns
        self.db = db
        self.catalog = catalog
        
    @property
    def qualified_name(self):
        n = self.name
        if self.db:
            n += '.'+self.db
            if self.catalog:
                n += '.' + self.catalog
        return n
        
    def to_dict(self):
        d = {
            'type': 'table',
            'type': self.type,
            'name': self.name
        }
        
        if self.db:
            d['db'] = self.db
        if self.catalog:
            d['catalog'] = self.catalog
        
        d['columns'] = {
            name: src.to_dict() if src else None
            for name, src in self.columns.items()
        }
        return d
    
class TransformSource(CompositeSource):
    def __init__(self, transform, sources, **kwargs):
        if sources and not type(sources) == list:
            sources = [sources]
            
        self.transform = transform
        super().__init__(sources=sources, **kwargs)
        
    def to_dict(self):
        d = {
            'type': 'transform',
            'transform': self.transform
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
            'table': self.table if type(self.table) == str else self.table.to_dict(),
            'column': self.column
        }
        d.update({k: v for k,v in super().to_dict().items() if k not in d})
        return d
        
class StructSource(Source):
    def __init__(self, name, source, **kwargs):
        Source.__init__(self, **kwargs)
        self.name = name
        self.source = source
        
    def to_dict(self):
        d = {
            'type': 'struct',
            'name': self.name,
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