from table import Table
import os
class Database():

    def __init__(self):
        self.tables = []
        self.path = 'ECS165'
        pass

    def open(self, path):  #set our database path to path
        self.path = path
        pass

    def close(self):  #put everything in bufferpool back to disk
        self.tables[0].bufferpool.close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
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
        pass
