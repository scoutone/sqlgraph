from sqlgraph.model import Table
import abc

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

            

    