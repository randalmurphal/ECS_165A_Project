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
        vals = []
        started = False
        for k, p_range in enumerate(self.table.page_directory):
            for base_pg in p_range.range[0]:
                for i, page in enumerate(base_pg.pages[4]):
                    for j in range(page.num_records):
                        val = page.retrieve(j)
                        if val == begin:
                            started = True
                        if started:
                            base_rid   = base_pg.pages[1][i].retrieve(j)
                            base_indir = base_pg.pages[0]
                            schema_enc = base_pg.pages[3][i]
                            if schema_enc[column]:
                                tail_rid    = base_indir[base_rid]
                                tail_page_i = tail_rid // 4096
                                tail_phys_page = (tail_rid % 4096) // 512
                                tail_rec    = tail_rid % 512
                                tail_page   = self.table.page_directory[k].range[1][tail_page_i].pages
                                # print("tail:",tail_page[column+4][tail_phys_page].retrieve(tail_rec))
                                # print("base:",base_pg.pages[column+4][i].retrieve(j))
                                vals.append(tail_page[column+4][tail_phys_page].retrieve(tail_rec))
                            else:
                                vals.append(base_pg.pages[column+4][i].retrieve(j))
                        if val == end:
                            return vals
        return vals

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
