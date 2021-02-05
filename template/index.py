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
    def locate(self, key, column, value):
        loc_vals = []
        key_dict = self.table.key_dict
        for key in key_dict:
            location = key_dict[key]
            p_range, base_pg, page, record = location
            base_pages = self.table.page_directory[p_range].range[0][base_pg].pages
            val = base_pages[column+4][page].retrieve(record)

            if val == value:
                loc_vals.append(location)
        return loc_vals

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        vals      = []
        key_dict  = self.table.key_dict
        b_loc     = key_dict[begin]
        b_p_range, b_base_pg, b_page, b_record = b_loc

        e_loc     = key_dict[end]
        e_p_range, e_base_pg, e_page, e_record = e_loc

        b_rid = self.table.page_directory[b_p_range].range[0][b_base_pg].pages[1][b_page].retrieve(b_record)
        e_rid = self.table.page_directory[e_p_range].range[0][e_base_pg].pages[1][e_page].retrieve(e_record)

        # Check through all key values to see if rid is between begin and end rid
        for key in key_dict:
            curr_loc = key_dict[key]
            curr_p_range, curr_base_pg, curr_page, curr_record = curr_loc
            curr_pages = self.table.page_directory[curr_p_range].range[0][curr_base_pg].pages
            curr_rid = curr_pages[1][curr_page].retrieve(curr_record)
            # Check indirection
            curr_ind = 512*curr_page + curr_record
            curr_schema = curr_pages[3][curr_ind]
            # if the column has been updated
            if curr_rid >= b_rid and curr_rid <= e_rid:
                if int(curr_schema[column]):
                    curr_indirection = curr_pages[0]
                    tail_rid     = curr_indirection[curr_rid]
                    tail_p_range = curr_p_range
                    tail_base_pg = tail_rid // 4096
                    tail_page    = (tail_rid % 4096) // 512
                    tail_record  = tail_rid % 512
                    curr_val     = self.table.page_directory[tail_p_range].range[1][tail_base_pg].pages[column+4][tail_page].retrieve(tail_record)
                    vals.append(curr_val)
                else:
                    curr_val = curr_pages[column+4][curr_page].retrieve(curr_record)
                    vals.append(curr_val)
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
