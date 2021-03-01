from template.table import Table, Record
from template.index import Index
from template.conceptual_page import ConceptualPage
from template.page_range import PageRange
from template.page import Page
from template.bufferpool import BufferPool

from random import randint
import numpy as np
import time, math, pickle, re, os
from datetime import datetime
import copy
MAX_INT = int(math.pow(2, 63) - 1)
MAX_PAGE_RANGE_SIZE = 8192
MAX_BASE_PAGE_SIZE  = 512
MAX_PHYS_PAGE_SIZE  = 512

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table       = table
        self.buffer_pool = table.buffer_pool

    """
        Read a record with specified RID
        Returns True if succes, False if rec doesn't exist or locked
        - Deleted if schema all 0's and indirection points to any tail page
    """
    def delete(self, key):
        # Make sure cpage is in buffer
        base_page = self.get_from_disk(key=key)
        base_page.isPinned = True
        _, is_in_buffer = self.in_buffer(base_page.path)
        if not is_in_buffer:
            # if not in buffer, add to buffer
            self.buffer_pool.addConceptualPage(base_page)
        _, _, _, base_rec_ind = self.buffer_pool.meta_data.key_dict[key]
        # Set schema to be 0's
        for i in range(len(base_page.pages[3][base_rec_ind])):
            base_page.pages[3][base_rec_ind][i] = 0
        # Set indirection to new tail page with all None values
        self.update(key, *[None]*self.table.num_columns)
        return True

    """
        Insert a record with specified columns
        Return True upon succesful insertion
        Returns False if insert fails for whatever reason
    """
    # Tuple of columns(Key,Value)
    def insert(self, *columns):
        key = columns[0]
        new_page_range, new_base_page = self.get_new_bools()
        # if we need to create a new page range
        if new_page_range:
            cpage = self.add_new_pr(*columns)
            cpage.isPinned = True
        else:
            # if need new conceptual page, create in currpr directory
            if new_base_page:
                cpage = self.add_new_bp(*columns)
                cpage.isPinned = True
            else:
                cpage = self.insert_to_bp(*columns)
                cpage.isPinned = True
        self.add_meta(cpage)
        self.insert_buffer_meta(key)
        cpage.isPinned = False
        cpage.dirty    = True
        return True

    ### ******* Insert Helpers ******* ###
    '''
        Adds a new page range directory & adds a new base page to buffer_pool
            with updated paths
        - returns new base page
    '''
    def add_new_pr(self, *columns):
        self.buffer_pool.meta_data.currpr += 1
        self.buffer_pool.meta_data.currbp = 0
        path = self.create_pr_dir() + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage = self.buffer_pool.createConceptualPage(path, False, *columns)
        return cpage

    '''
        Creates new base page and adds to bufferpool & pins it
    '''
    def add_new_bp(self, *columns):
        self.buffer_pool.meta_data.currbp += 1
        path = './template/ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage = self.buffer_pool.createConceptualPage(path, False, *columns)
        return cpage

    '''
        Insert record into bp that already exists
    '''
    def insert_to_bp(self, *columns):
        path = './template/ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            cpage = self.get_from_disk(key, path)
            self.buffer_pool.addConceptualPage(cpage)
        self.buffer_pool.populateConceptualPage(columns, cpage)
        return cpage

    '''
        Get bool vals for if we need:
            - new page range, new base page
    '''
    def get_new_bools(self):
        new_pr = self.buffer_pool.meta_data.baseRID_count  % MAX_PAGE_RANGE_SIZE == 0
        new_bp = self.buffer_pool.meta_data.baseRID_count  % MAX_PHYS_PAGE_SIZE  == 0
        return new_pr, new_bp

    '''
        Get location vals for record:
            - (pr#, bp#, pg_ind, rec_ind)
    '''
    def get_loc(self):
        pr_ind = self.buffer_pool.meta_data.currpr
        bp_ind = self.buffer_pool.meta_data.currbp
        p_ind  = (self.buffer_pool.meta_data.baseRID_count % MAX_BASE_PAGE_SIZE) // MAX_PHYS_PAGE_SIZE
        r_ind  = self.buffer_pool.meta_data.baseRID_count  % MAX_PHYS_PAGE_SIZE
        return pr_ind, bp_ind, p_ind, r_ind

    '''
        Inserts new key:location to key dict map and increment base_rid count
    '''
    def insert_buffer_meta(self, key):
        self.buffer_pool.meta_data.key_dict[key] = self.get_loc()
        self.buffer_pool.meta_data.baseRID_count += 1

    ### ******* End of Insert Helpers ******* ###
# else:
#     # for key in self.buffer_pool.meta_data.key_dict.keys():
#
#
#     # loop through every goddamn base page in the whole damn database
#     # for key in self.bufferpool.meta_data.key_dict.keys()
#     # cpage=self.get_from_disk(key=key)
#     rec_index_list=[]
#     records_return_list=[]
#     rec_index=0
#     for rec_num in range(cpage.num_records):
#         rec_key = cpage.pages[6+column].retrieve(rec_num)
#         if rec_key == key:
#             rec_index = rec_num
#             rec_index_list.append(rec_index)
#     for rec_index in rec_index_list:
#         rec_RID = cpage.pages[1].retrieve(rec_index)
#         updated = False
#         indirection = cpage.pages[0]
#         if rec_RID in indirection.keys():
#             # DO SHIT WITH UPDATES SHIT HERE
#             tail_RID = indirection[rec_RID]
#             tail_pg  = self.getTailPage(tail_RID)
#             # Get values at schema encoded columns
#         else:
#             # if base record has not been updated: return base record
#             values_to_return = []
#             # checks query columns to see which columns we want to return. If 0: return None, else: return value
#             for i in range(len(query_columns)):
#                 if query_columns[i] == 0:
#                     values_to_return.append(None)
#                 else:
#                     values_to_return.append(cpage.pages[i+6].retrieve(rec_index))
#                     records_return_list.append(Record(rec_RID, value, values_to_return))
    """
    Read a record with specified key
    :param key: the key value to select records based on
    :param query_columns: what columns to return. array of 1 or 0 values.
    Returns a list of Record objects upon success
    Returns False if record locked by TPL (milestone 3)
    Assume that select will never be called on a key that doesn't exist
    """

    def select(self, value, column, query_columns):
        records = []
        # if key column
        if column != 0:
            # get base page (checks if in buffer or not)
            key   = value
            cpage = self.get_from_disk(key=key)
            cpage.isPinned = True
            self.buffer_pool.addConceptualPage(cpage)
            # Get index of record based on key
            rec_index = self.get_rec_ind(cpage, key)
            rec_RID   = cpage.pages[1].retrieve(rec_index)
            indirection = cpage.pages[0]
            # if record has been updated => grab newest values from tail page
            if rec_RID in indirection.keys():
                record = self.get_updated_values(key, query_columns, cpage)
            # Hasnt been updated, grab all values according to query_columns
            else:
                record = self.get_base_values(key, query_columns, cpage)
            records.append(record)
            cpage.isPinned = False
        else:
            if self.index_exists(column):
                index = self.table.index.indices[column]
                if value in index.keys():
                    val_index = index[value]
                    for rec_val in val_index:
                        rec_ind, path = rec_val
                        record = self.get_from_index(rec_ind, path, query_columns)
                        records.append(record)
            else:
                base_paths = self.get_base_paths()
                # Search through entire disk for all base pages
                for path in base_paths:
                    record_vals = self.get_records(path, value, column, query_columns)
                    records = list(np.concatenate((records, record_vals)))
        return records

    ### Select Helpers ###
    '''
        Return record with vals from tail page if update, base page if not
    '''
    def get_updated_values(self, key, query_columns, cpage):
        values      = []
        rec_index   = self.get_rec_ind(cpage, key)
        rec_RID     = cpage.pages[1].retrieve(rec_index)
        indirection = cpage.pages[0]
        b_schema    = cpage.pages[3][rec_index]
        tail_path, tail_rec_ind = indirection[rec_RID]
        tail_page   = self.get_tail_page(cpage, key, True, *[None]*self.table.num_columns)
        tail_page.isPinned = True
        for i, val in enumerate(b_schema):
            if query_columns[i] == 1:
                if val == 0:
                    # grab from base
                    values.append(cpage.pages[6+i].retrieve(rec_index))
                else:
                    # grab from tail
                    values.append(tail_page.pages[6+i].retrieve(tail_rec_ind))
            else:
                values.append(None)
        tail_page.isPinned = False
        return Record(rec_RID, key, values)

    '''
        Returns record from key
    '''
    def get_base_values(self, key, query_cols, cpage):
        values    = []
        rec_index = self.get_rec_ind(cpage, key)
        base_RID  = cpage.pages[1].retrieve(rec_index)
        for i, val in enumerate(query_cols):
            if val == 1:
                values.append(cpage.pages[6+i].retrieve(rec_index))
            else:
                values.append(None)
        return Record(base_RID, key, values)

    '''
        Returns True if index in index_arr, else False
    '''
    def index_exists(self, col_num):
        if self.table.index.indices[col_num] != None:
            return True
        return False

    '''
        Returns record from index map
    '''
    def get_from_index(self, rec_ind, path, query_columns):
        base_page = self.get_from_disk(path=path)
        self.buffer_pool.addConceptualPage(base_page)
        base_page.isPinned = True
        base_RID     = base_page.pages[1].retrieve(rec_ind)
        key          = base_page.pages[6].retrieve(rec_ind)
        indirection  = base_page.pages[0]
        if base_RID in indirection.keys():
            record = self.get_updated_values(key, query_columns, base_page)
        else:
            record = self.get_base_values(key, query_columns, base_page)
        base_page.isPinned = False
        return record

    '''
        Returns record from path
    '''
    def get_records(self, path, value, column, query_columns):
        records = []
        base_page = self.get_from_disk(path=path)
        base_page.isPinned = True
        self.buffer_pool.addConceptualPage(base_page)
        indirection = base_page.pages[0]
        # for each record in base page
        for rec_ind in range(base_page.num_records):
            rec_RID = base_page.pages[1].retrieve(rec_ind)
            key = base_page.pages[6].retrieve(rec_ind)
            if rec_RID in indirection.keys():
                record = self.get_updated_values(key, query_columns, base_page)
                if record.columns[column] == value:
                    records.append(record)
            else:
                record = self.get_base_values(key, query_columns, base_page)
                if record.columns[column] == value:
                    records.append(record)
        base_page.isPinned = False
        return records

    ### End of Select Helpers ###

    """
    Update a record with specified key and columns
    Returns True if update is succesful
    Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # """---New---"""
        # columns_to_update = self.get_query_cols(key, *columns)
        my_base_page = self.get_from_disk(key=key)
        base_page, is_in_buffer = self.in_buffer(my_base_page.path)
        if not is_in_buffer:
            self.buffer_pool.addConceptualPage(my_base_page)
        else:
            my_base_page = base_page
        my_base_page.isPinned = True
        tail_page = self.get_tail_page(my_base_page, key, False, *columns)
        self.update_tail_page(my_base_page, tail_page, key, *columns)
        my_base_page.isPinned = False
        my_base_page.dirty    = True
        return True

    ### ******* Update Helpers ******* ###

    '''
        Updates a tail record for a new update on a base record
        - On first update for any column, add snapshot with the update
    '''
    def update_tail_page(self, base_page, tail_page, key, *columns):
        tail_page.isPinned = True
        rec_ind       = self.buffer_pool.meta_data.key_dict[key][3]
        base_rec_RID       = base_page.pages[1].retrieve(rec_ind)
        base_indirection   = base_page.pages[0]
        new_schema = copy.copy(base_page.pages[3][rec_ind])
        old_schema = base_page.pages[3][rec_ind]
        # query_cols = self.get_query_cols(key, *columns)
        query_cols = self.update_schema(new_schema, *columns)
        prev_tail_values = []
        # check if base record has been updated previously, if it has then grab the values from the previous update.
        if base_rec_RID in base_indirection.keys():
            prev_tail_values = self.get_prev_tail(key, rec_ind, base_rec_RID, base_page, *columns) # base_rec_RID, base_indirection)
        # If first update for any column, save snapshot
        if not np.array_equal(old_schema, new_schema):
            self.create_snapshot(old_schema, new_schema, rec_ind, base_page, tail_page)
            # if full after creating snapshot
            if tail_page.full():
                tail_page.isPinned = False
                tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned = True
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, *columns)
        else: #If we don't create a snapshot
            # If full, create new tail page
            if tail_page.full():
                tail_page.isPinned = False
                pr_num    = base_page.path.split("/")[3][2:]
                tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned = True
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, *columns)

        base_page.pages[3][rec_ind] = new_schema
        tail_rec_ind     = tail_page.num_records - 1
        tail_path        = tail_page.path
        base_page.pages[0][base_rec_RID] = tail_path, tail_rec_ind
        tail_page.isPinned = False
        return True

    '''
        Writes snapshot to tail page
    '''
    def create_snapshot(self, old_schema, new_schema, rec_ind, base_page, tail_page):
        snapshot_col = []
        # Goes through to check which schema different from last
        for i in range(len(old_schema)):
            if new_schema[i] != old_schema[i]:
                snapshot_col.append(1)
            else:
                snapshot_col.append(0)
        # makes sure to update RID and tail page record num for the snapshot
        for i, val in enumerate(snapshot_col):
            if val == 1:
                tail_page.pages[i+6].write(base_page.pages[i+6].retrieve(rec_ind))
            else:
                tail_page.pages[i+6].write(MAX_INT)
        tail_page.num_records += 1
        self.buffer_pool.meta_data.tailRID_count += 1

    '''
        Writes tail record to tail page
    '''
    def create_tail(self, new_schema, prev_tail_values, tail_page, *columns):
        for i, val in enumerate(new_schema):
            # If page has update at column i
            if val == 1:
                if columns[i] == None:
                    # Set value to prev tail page value
                    tail_page.pages[i+6].write(prev_tail_values[i])
                else:
                    # Set value to updated value
                    tail_page.pages[i+6].write(columns[i])
            else:
                # Set value to None/MaxInt
                tail_page.pages[i+6].write(MAX_INT)
        # UPDATE schema_col
        tail_page.num_records += 1
        self.buffer_pool.meta_data.tailRID_count += 1
        return tail_page
    '''
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Used through the bufferpool
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    '''

    '''
        Updates schema & returns the query columns to index updated cols
    '''
    def update_schema(self, base_schema, *columns):
        # Get query cols from columns
        query_cols = []
        for i, col in enumerate(columns):
            if col != None:
                query_cols.append(1)
            else:
                query_cols.append(0)
        # update the schema according to query cols
        for i, col in enumerate(query_cols):
            if col == 1:
                base_schema[i] = 1
        return query_cols

    '''
        Get tail page, even if it is full
        - Assumes base_rid in base_indirection already
        - Returns values so it is easier to process
    '''
    def get_prev_tail(self, key, rec_ind, base_rid, base_page, *columns):
        base_rec_RID     = base_page.pages[1].retrieve(rec_ind)
        base_indirection = base_page.pages[0]
        prev_tail_path, prev_tail_rec_ind = base_indirection[base_rid]
        prev_tail_values = []
        prev_tail_pg     = self.get_tail_page(base_page, key, True, *columns)
        prev_tail_pg.isPinned = True
        for i in range(len(columns)):
            prev_tail_values.append(prev_tail_pg.pages[6+i].retrieve(prev_tail_rec_ind))
        prev_tail_pg.isPinned = False
        return prev_tail_values

    ### ******* End of Update Helpers ******* ###

    '''
        Sum all records for the aggregate_column_index between start & end vals
        - returns sum
    '''
    def sum(self, start_range, end_range, aggregate_column_index):
        file_paths = self.get_base_paths() # gets all base page paths
        sum = 0
        start_range, end_range = self.start_is_less(start_range, end_range)
        # Check through every base page & their records
        for path in file_paths:
            # get base page from path
            cpage = self.get_from_disk(path=path)
            cpage.isPinned = True
            _, is_in_buffer = self.in_buffer(path) #checks if its in bufferpool conceptualPage list
            if not is_in_buffer:
                self.buffer_pool.addConceptualPage(cpage) #add to bufferpool
            # check indirection to see if tail page is in indirection
            for i in range(cpage.num_records):
                curr_val         = cpage.pages[aggregate_column_index+6].retrieve(i)
                if self.rec_is_deleted(i, cpage):
                    continue
                if start_range <= curr_val <= end_range:
                    sum += self.add_sum(i, aggregate_column_index, cpage)
            cpage.isPinned = False
        return sum


    ### ******* Sum Helpers ******* ###

    '''
        Gets all base page paths in disk and buffer_pool
            - Uses regex to match with all base pages when searching in disk/buf
        returns paths
    '''
    def get_base_paths(self):
        regex = re.compile("./template/ECS165/%s/PR[0-9]+/BP[0-9]+"%self.table.name)
        rootdir = './template/ECS165/'
        file_paths = []
        # Check in disk
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                path = os.path.join(subdir, file) # path to file
                # if it matches with some value within the current path, append
                if not re.match(regex, path) == None:
                    file_paths.append(path)
        # Check in buffer_pool
        for path in self.buffer_pool.buffer_keys.keys():
            if not re.match(regex, path) == None:
                file_paths.append(path)
        return list(set(file_paths)) # removes duplicates

    '''
        Makes sure start range is less than end range, if not then swap
    '''
    def start_is_less(self, start_range, end_range):
        if start_range > end_range:
            temp  = copy.copy(start_range)
            start_range = copy.copy(end_range)
            end_range = temp
        return start_range, end_range

    '''
        Checks to see if a record is deleted
        - deleted if schema all 0's and indirection points to a tail record
    '''
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

    '''
        Returns value to be added
        - if record is deleted, return 0
        - if updated, grab from tail. else grab from base
    '''
    def add_sum(self, rec_num, col_ind, cpage):
        # Dont sum if record has been deleted
        # if self.rec_is_deleted(rec_num, cpage):
        #     return 0

        indirection = cpage.pages[0]
        rec_RID     = cpage.pages[1].retrieve(rec_num)
        b_schema    = cpage.pages[3][rec_num]
        # checks if the has indirection & updates been made for that column
        updated = rec_RID in indirection.keys() and b_schema[col_ind] == 1
        if updated:
            tail_path, tail_rec_ind = indirection[rec_RID]
            tail_page = self.get_from_disk(path=tail_path)
            tail_page.isPinned = True
            self.buffer_pool.addConceptualPage(tail_page)
            tail_page.isPinned = False
            return tail_page.pages[col_ind+6].retrieve(tail_rec_ind)
        else:
            return cpage.pages[col_ind+6].retrieve(rec_num)
    ### ******* End of Sum Helpers ******* ###

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    '''
        Gets tail page from buffer or disk
        - if prev = True, then return tail even if it is full
        - if prev = False, return new tail if prev is full
    '''
    def get_tail_page(self, base_page, key, prev, *columns):
        base_page.isPinned = True
        indirection  = base_page.pages[0]
        base_rec_ind = self.buffer_pool.meta_data.key_dict[key][3] # record num in bp
        base_rec_RID = base_page.pages[1].retrieve(base_rec_ind)   # RID of base record
        pr_num       = base_page.path.split('/')[4]                # keep same PR as base page for tail
        # If tail page exists, get values from indirection. Else create new tail path
        if base_rec_RID in indirection.keys():
            tail_path, tail_rec_ind = indirection[base_rec_RID]
        else:
            tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
        # if tail not in buffer, get from file and add to buffer_pool. Else just grab from buffer_pool
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            # The first tail page for the table should create a new one
            if not self.buffer_pool.first:
                if self.tail_in_indirection(tail_path, indirection):
                    tail_page = self.get_from_disk(key, tail_path)
                    # prev = false for new tail page full & not looking for prev tail values
                    if tail_page.full() and prev == False:
                        tail_page = self.new_tail_page(pr_num, *columns)
                    else:
                        self.buffer_pool.addConceptualPage(tail_page)
                else:
                    tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                    self.add_meta(tail_page)
            else:
                # the first page -- create & add to buffer_pool
                self.buffer_pool.first = False
                tail_page = self.new_tail_page(pr_num, *columns)
        else:
            if tail_page.full() and prev == False:
                tail_page = self.new_tail_page(pr_num, *columns)
        base_page.isPinned = False
        return tail_page

    '''
        Gets a page from disk
        - if no path, get path from key
    '''
    def get_from_disk(self, key=None, path=None):
        # If no path, get path for base page from key
        if path == None:
            if key == None:
                raise TypeError("Key value is None: get_from_disk()")
            location = self.buffer_pool.meta_data.key_dict[key]
            path     = './template/ECS165/'+self.table.name+'/PR'+str(location[0])+'/BP'+str(location[1])
        # if not in buffer, grab from disk
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'rb') as db_file:
                cpage = pickle.load(db_file)
        return cpage

    '''
        Creates new tail page from meta_data path & add to buffer_pool
    '''
    def new_tail_page(self, pr_num, *columns):
        tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count // 512))
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
        self.add_meta(tail_page)
        return tail_page

    '''
        Creates a new page range directory in table dir
        Returns path to page range directory
    '''
    def create_pr_dir(self):
        pr_path = os.path.join('./template/ECS165/'+self.table.name, "PR"+str(self.buffer_pool.meta_data.currpr))
        os.mkdir(pr_path)  #insert new PR and BP into disk
        return pr_path

    '''
        Checks to see if a cpage is in buffer
        - returns cpage, True if it is // None, False if it isn't
    '''
    def in_buffer(self, path):
        for cpage in self.buffer_pool.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False

    '''
        Adds values to columns[0:5] when creating a cpage
    '''
    def add_meta(self, cpage):
        # Get current time value
        current_time = datetime.now().time()
        time_val     = ""
        hour         = 0
        # Extract hour * 60, then add to minutes to get total current time in minutes
        for digit in current_time.strftime("%H:%M"):
            if not digit == ":":
                time_val = time_val + digit
            else:
                hour = int(time_val) * 60
                time_val = ""
        time_val = int(time_val) + hour
        values = [self.buffer_pool.meta_data.baseRID_count, time_val]
        cpage.pages[1].write(values[0])  # Base_RID col
        cpage.pages[2].write(values[1])  # Timestamp col
        cpage.pages[3].append(np.zeros(len(cpage.pages) - 6)) # Schema
        cpage.pages[4].write(0) # Other Base_RID
        cpage.pages[5].write(0) # TPS col

    '''
        Returns query columns for which columns need to be updated
    '''
    def get_query_cols(self, key, *columns):
        query_columns = []
        for i, col in enumerate(columns):
            if col != None:
                query_columns.append(1)
            else:
                query_columns.append(0)
        return query_columns

    '''
        Returns True if tail record in base record indirection, else False
    '''
    def tail_in_indirection(self, tail_path, indirection):
        for val in indirection.values():
            if tail_path == val[0]:
                return True

        return False

    '''
        Returns record index in base page based on key if exists, else -1
    '''
    def get_rec_ind(self, cpage, key):
        for rec_num in range(cpage.num_records):
            rec_key = cpage.pages[6].retrieve(rec_num)
            if rec_key == key:
                return rec_num
        return -1
