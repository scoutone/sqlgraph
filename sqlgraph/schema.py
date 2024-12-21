import abc

class Table():
    def __init__(self, table, columns, db=None, catalog=None):
        self.table = table
        self.db = db
        self.catalog = catalog
        self.columns = columns
       
    @property 
    def id(self):
        s = self.table
        if self.db:
            s = self.db + '.' + s
            if self.catalog:
                s = self.catalog + '.' + s
        return s
        
    def __str__(self):
        return self.id
    
    def __repr__(self):
        return 'Table['+self.id+']'
    
    def __eq__(self, other):
        return isinstance(other, Table) and \
               self.table == other.table and \
               self.db == other.db and \
               self.catalog == other.catalog
    
    def __ne__(self, other):
        return not self.__eq__(other)

class Schema(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractclassmethod
    def get_table(self, table, db=None, catalog=None):
        pass
    
class DictSchema(Schema):
    def __init__(self, schema_dict):
        self.schema_dict = schema_dict

    def get_table(self, table, db=None, catalog=None):
        matches = []
        for catalog_name, catalogs in self.schema_dict.items():
            if catalog is not None and catalog_name != catalog:
                continue
            for db_name, tables in catalogs.items():
                if db is not None and db_name != db:
                    continue
                if table in tables:
                    matches.append(Table(table, tables[table], db_name, catalog_name))
                    
        if len(matches) > 1:
            raise ValueError(f'matched multiple tables for {catalog}.{db}.{table}: {[str(m) for m in matches]}')
        return matches[0] if matches else None

            

    