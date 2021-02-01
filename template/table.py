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
        # self.page_ranges = []
        self.name = name
        self.key = key
        # Key is an RID
        self.num_columns = num_columns
        # Cols are RID,Indirection,Schema Encoding,TimeStamp,{num_columns}
        # Page_directory stores the basepages and to find the base page you want
        # Base Pages: RID // 4906 , Physical Page:
        self.page_directory = []
        # Each time u create a base page store it in the page_directory
        # Given RID, return a page based off of the RID
        self.index = Index(self)
        # self.page_ranges.append(PageRange())
        self.RID_count = 0

    def __merge(self):
        pass
