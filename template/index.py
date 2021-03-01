import os, re
import numpy as np
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
            cols = []
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
        # # For when main asks for 10k + 1 key, but only inserted 10k keys
        # if end == self.table.init_key + self.table.RID_count:
        #     end -= 1
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
        Creates an index for specified column
            - column_value -> [paths with this value]
    """

    def create_index(self, column_number):
        index = {} # map from value: [(rec_ind, page_path), ...)]
        # Get all base pages
        base_paths = self.get_base_paths()
        # For each value/updated valkue in column index, add to dict
        for path in base_paths:
            base_page = self.get_page(path)
            for rec_ind in range(base_page.num_records):
                tail = self.get_tail_page(rec_ind, base_page)
                base_schema = base_page.pages[3][rec_ind]
                if tail==None or base_schema[column_number]==0:
                    # check value from base
                    if not self.rec_is_deleted(rec_ind, base_page):
                        value = base_page.pages[column_number+6].retrieve(rec_ind)
                        if value not in index.keys():
                            index[value] = []
                        index[value].append((rec_ind, path))
                else:
                    tail_page, tail_rec_ind = tail
                    # Grab value from tail -> base path
                    value = tail_page.pages[column_number+6].retrieve(tail_rec_ind)
                    # If not already in index, add array to append paths to
                    if value not in index.keys():
                        index[value] = []
                    index[value].append((rec_ind, path))

        self.table.index.indices[column_number] = index

    ### ******* Create_Index Helpers ******* ###

    '''
        Returns all paths for all base pages in disk & buffer
    '''
    def get_base_paths(self):
        regex = re.compile("./template/ECS165/%s/PR[0-9]+/BP[0-9]+"%self.table.name)
        rootdir = './template/ECS165/'
        base_paths = []
        # Check in disk
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                path = os.path.join(subdir, file)
                # if it matches with some value within the current path, append
                if not re.match(regex, path) == None:
                    base_paths.append(path)
        # Check in buffer_pool
        for path in self.table.buffer_pool.buffer_keys.keys():
            if not re.match(regex, path) == None:
                base_paths.append(path)
        return list(set(base_paths))

    '''
        Returns tail page & its index if it is in base_page indirection, else None
    '''
    def get_tail_page(self, rec_ind, base_page):
        # key          = base_page.pages[6].retrieve(rec_ind)
        # base_rec_ind = self.table.buffer_pool.meta_data.key_dict[key][3]
        indirection = base_page.pages[0]
        base_RID    = base_page.pages[1].retrieve(rec_ind)
        if base_RID not in indirection.keys():
            return None
        tail_path, tail_rec_ind = indirection[base_RID]
        return self.get_page(tail_path), tail_rec_ind

    '''
        Returns page from disk
    '''
    def get_page(self, path):
        # if not in buffer, grab from disk
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'rb') as db_file:
                cpage = pickle.load(db_file)
        return cpage

    '''
        Returns page, True if in buffer, else None, False
    '''
    def in_buffer(self, path):
        for cpage in self.table.buffer_pool.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False

    def rec_is_deleted(self, rec_num, cpage):
        indirection = cpage.pages[0]
        rec_RID     = cpage.pages[1].retrieve(rec_num)
        b_schema    = cpage.pages[3][rec_num]
        for val in b_schema:
            if val == 1:
                return False
        if rec_RID in indirection.keys():
            return True
        return False

    ### ******* End of Create_Index Helpers ******* ###

    """
        Remove an index from Index.indices (set index to None)
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
