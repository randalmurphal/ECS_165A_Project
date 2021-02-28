from table import Table
from bufferpool import BufferPool
import os, pickle
class Database():

    def __init__(self):
        self.tables = []
        self.path = 'ECS165'
        pass

    def open(self, path):  #set our database path to path
        self.path = path
        pass

    def close(self):  #put everything in bufferpool back to disk
        while self.tables:
            table = self.tables.pop(0)
            t_path = './ECS165/%s/table'%table.name
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
        table = Table(name, num_columns, key)
        table.buffer_pool = BufferPool(name)
        self.tables.append(table)
        "NEW - makes a table directory with name in ECS 165"
        os.mkdir(os.path.join(self.path+'/', name))
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
        rootdir = './ECS165/'
        for subdir, dirs, files in os.walk(rootdir):
            for table_name in dirs:
                if table_name == name:
                    t_path = './ECS165/%s/table'%table_name
                    with open(t_path, 'rb') as t_file:
                        table = pickle.load(t_file)
                    return table

        raise ValueError("Table name '%s' not in directory '%s'"%(name, rootdir))

            # if dirs == name:
            # for file in files:
            #     path = os.path.join(subdir, file) # path to file
            #     if not re.match(regex, path) == None:
            #         file_paths.append(path)
