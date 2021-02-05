from page import *
from index import Index
from time import time
# from page_range import PageRange

# These are indexes
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# Database -> Tables of diff classes -> Page Range -> Column of Data(Pages)
class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        # Want a page per num_columns
        self.name = name
        self.key = key
        # Key is an RID
        self.num_columns = num_columns
        # Page_directory stores the basepages and to find the base page you want
        self.page_directory = []
        self.key_dict = {}
        # Given RID, return a page based off of the RID
        self.index = Index(self)
        self.RID_count = 0
        self.init_key  = 0

    def __merge(self):
        pass
