"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default,
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        loc_vals = []
        for i, p_range in enumerate(self.table.page_directory):
            for j, base_pg in enumerate(p_range.range[0]):
                for k, page in enumerate(base_pg.pages[column+4]):
                    for l in range(page.num_records):
                        val = page.retrieve(l)
                        # print(val, value)
                        if val == value:
                            loc_vals.append((i, j, k, l))
        return loc_vals

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rid_vals = []
        for range in self.table.page_directory:
            for base_pg in range.range[0]:
                for i in range(len(base_pg)):
                    for l in range(page.num_records):
                        val = page.retrieve(l)
                        if val >= begin and val <= end:
                            rid_vals.append(base_pg[1].retrieve(i))
        return rid_vals

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
