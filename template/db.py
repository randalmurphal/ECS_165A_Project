from table import Table

import pickle
# Open/Close -- saving and loading from file with pickle
    # dont load the whole table when opening bc max bufferpool size
    # Figure out bufferpool management data structures
        # How to write/read to a file at a specific location, without loading entire database?? (pool of memory pages)
            # keep track of dirty pages
            # Write all pages to file, we can know which index of bytes it is by an indexing structure??
        # When full, write back pages to disk (if no query queued for that record) to make room for new pages we want to read from
    # fixed number of pages in bufferpool = 16 or 32
# Add indexing for data columns, not just key dictionary

#bufferpool class
#starts w/ nothing
#array of tail and base pages
    #counter in both, to keep the num of pages
    #store a whole table in bufferpool

    # Functions:
    # add/fetch page (pin page)
    # evict pages (unpin page) (if dirty)
    # re-store updated pages
    # update/write to page

# Merging
    # Merge every X number of updates

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self, path):
        ### init bufferpool object ###
        # try to open file, if doesnt exist create it
        try:
            with open(path, 'r') as db_file:
                self.tables = pickle.load(path)
        except IOError:
            with open(path, 'w+') as db_file:
                pickle.dump(self.tables, db_file)


    def close(self):
        # Write all changes to disk from bufferpool object
        #Also merge?
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
