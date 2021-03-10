from template.table import Table
from template.bufferpool import BufferPool
import os, pickle
class Database():

    def __init__(self):
        self.tables = []

    def open(self, path):  #set our database path to path
        self.path = path[2:]

    def close(self):  #put everything in bufferpool back to disk
        while self.tables:
            table = self.tables.pop(0)
            t_path = './template/%s/%s/table'%(self.path,table.name)
            table.buffer_pool.close() # evict all
            # Store table in file (with bufferpool in it)
            with open(t_path, 'wb') as t_file:
                pickle.dump(table, t_file)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.path)
        table.buffer_pool = BufferPool(name, num_columns)
        self.tables.append(table)
        "NEW - makes a table directory with name in ECS 165"
        os.mkdir("./template/"+self.path)
        os.mkdir(os.path.join("./template/"+self.path+'/', name))
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)
                return

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        rootdir = './template/%s/' %self.path
        for subdir, dirs, files in os.walk(rootdir):
            for table_name in dirs:
                if table_name == name:
                    t_path = './template/%s/%s/table'%(self.path,table_name)
                    with open(t_path, 'rb') as t_file:
                        table = pickle.load(t_file)
                    return table

        raise ValueError("Table name '%s' not in directory '%s'"%(name, rootdir))
