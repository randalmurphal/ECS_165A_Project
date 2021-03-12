from template.table import Table, Record
from template.index import Index
from template.conceptual_page import ConceptualPage
from template.page_range import PageRange
from template.page import Page
from template.bufferpool import *
from template.logger import Logger
from template.lock_manager import LockManager
from random import randint

import threading
import numpy as np
import time, math, pickle, re, os
from datetime import datetime
import copy, json

MAX_INT = int(math.pow(2, 63) - 1)
MAX_PAGE_RANGE_SIZE = 65536
MAX_BASE_PAGE_SIZE  = 512
MAX_PHYS_PAGE_SIZE  = 512
buffer_lock = threading.Lock()
insert_lock = threading.Lock()
lock_lock   = threading.Lock()

#ISSUES :
#CURRENT
#seems to be a problem whenever evict() is called in addconceptual
#why is it inserting to baserid = 7k+, not using threads properly to insert, insert works fine in other tester
#FOR FUTURE
#logger is not properly logging

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
        self.logger      = Logger()
        self.trans_num   = 0

    """
        Read a record with specified RID
        Returns True if succes, False if rec doesn't exist or locked
        - Deleted if schema all 0's and indirection points to any tail page
    """
    def delete(self, key):
        # Make sure cpage is in buffer
        # print("buffer_lock1") # lock thread
        # print("buffer_lock1 acquired") # lock thread
        base_page = self.get_from_disk(key=key)
        if base_page == None:
            return False
        buffer_lock.acquire()
        _, is_in_buffer = self.in_buffer(base_page.path)
        if not is_in_buffer:
            # if not in buffer, add to buffer
            self.buffer_pool.addConceptualPage(base_page)
        # print("buffer_lock1 released")
        buffer_lock.release()
        base_rec_ind = self.buffer_pool.meta_data.key_dict[key][-1]
        # Set schema to be 0's
        for i in range(len(base_page.pages[3][base_rec_ind])):
            base_page.pages[3][base_rec_ind][i] = 0
        # Set indirection to new tail page with all None values
        self.update(key, *[None]*self.table.num_columns)
        base_page.isPinned -= 1
        return True

    """
        Insert a record with specified columns
        Return True upon succesful insertion
        Returns False if insert fails for whatever reason
    """
    # Tuple of columns(Key,Value)
    def insert(self, *columns):
        # print("insert: ", threading.get_ident(), *columns, "\n")
        # print("inserting: \n", json.dumps(self.table.lock_manager.lock_recs))
        # Check if the thread_num && key are in lock_manager
        lock_manager = self.table.lock_manager.lock_recs
        trans_num = threading.get_ident()
        key = columns[0]
        insert_lock.acquire()
        # buffer_lock.acquire()
        lock_lock.acquire()
        query_type = "insert"
        if self.locked_for_write(query_type, key):
            # Insert is a write operation, we abort on read/write
            insert_lock.release()
            lock_lock.release()
            # print("1")
            return False
        else:
            self.table.lock_manager.add_lock(key, "Write", trans_num)
        lock_lock.release()
        new_page_range, new_base_page = self.get_new_bools()
        # if we need to create a new page range
        if new_page_range:
            #print("creating new PR because RID count is: ", self.buffer_pool.meta_data.baseRID_count )
            cpage = self.add_new_pr(*columns)
            if cpage == None:
                insert_lock.release()
                # print("2")
                return False
        else:
            # if need new conceptual page, create in currpr directory
            if new_base_page:
                cpage = self.add_new_bp(*columns)
                if cpage == None:
                    insert_lock.release()
                    # print("3")
                    return False
            else:
                cpage = self.insert_to_bp(*columns)
                if cpage == None:
                    insert_lock.release()
                    # print("4")
                    return False
        self.buffer_pool.meta_data.baseRID_count += 1
        cpage.dirty = True
        cpage.isPinned -= 1
        # buffer_lock.release()
        insert_lock.release()
        # grab location of record for updating index
        rec_loc = self.buffer_pool.meta_data.key_dict[key]
        rec_page_index = rec_loc[-1]
        # check which columns currently have indexes
        cols_with_index = [col_num for col_num in range(len(columns)) if self.table.index.indices[col_num] != None]
        # add values from a given column in the inserted record to it's index if that index exists
        for col_num in cols_with_index:
            col_val = columns[col_num]
            col_index = self.table.index.indices[col_num]
            try:
                col_index[col_val].append((rec_page_index, cpage.path))
            except:
                col_index[col_val] = [(rec_page_index, cpage.path)]
        # print("insert_done: ", threading.get_ident(), *columns, "\n")
        return True

    ### ******* Insert Helpers ******* ###
    '''
        Adds a new page range directory & adds a new base page to buffer_pool
            with updated paths
        - returns new base page
    '''
    def add_new_pr(self, *columns):
        # print("b1")
        buffer_lock.acquire()
        # print("b1 acquired")
        self.buffer_pool.meta_data.currbp = 0
        self.buffer_pool.meta_data.currpr += 1
        path = self.create_pr_dir() + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage = self.buffer_pool.createConceptualPage(path, False, *columns)
        cpage.isPinned += 1
        self.add_meta(cpage)
        self.insert_buffer_meta(columns[0])
        # print("b1 released")
        buffer_lock.release()
        return cpage

    '''
        Creates new base page and adds to bufferpool & pins it
    '''
    def add_new_bp(self, *columns):
        buffer_lock.acquire()
        self.buffer_pool.meta_data.currbp += 1
        path  = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage = self.buffer_pool.createConceptualPage(path, False, *columns)
        cpage.isPinned += 1
        self.add_meta(cpage)
        self.insert_buffer_meta(columns[0])
        buffer_lock.release()
        return cpage

    '''
        Insert record into bp that already exists
    '''
    def insert_to_bp(self, *columns):
        path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
        cpage = self.get_from_disk(path=path)
        if cpage == None:
            # buffer_lock.release()
            return None
        buffer_lock.acquire()
        self.buffer_pool.populateConceptualPage(columns, cpage)
        self.add_meta(cpage)
        self.insert_buffer_meta(columns[0])
        # print("b3 released")
        buffer_lock.release()
        return cpage

    '''
        Get bool vals for if we need:
            - new page range, new base page
    '''
    def get_new_bools(self):
        new_pr = self.buffer_pool.meta_data.baseRID_count % MAX_PAGE_RANGE_SIZE == 0
        new_bp = self.buffer_pool.meta_data.baseRID_count % MAX_PHYS_PAGE_SIZE  == 0
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

    ### ******* End of Insert Helpers ******* ###

    """
    Read a record with specified key
    :param key: the key value to select records based on
    :param query_columns: what columns to return. array of 1 or 0 values.
    Returns a list of Record objects upon success
    Returns False if record locked by TPL (milestone 3)
    Assume that select will never be called on a key that doesn't exist
    """

    def select(self, value, column, query_columns):
        trans_num = threading.get_ident()
        records   = []
        lock_manager = self.table.lock_manager.lock_recs
        # if key column
        if column == 0:
            key = value
            # get base page (checks if in buffer or not)
            # print("b0")
            buffer_lock.acquire()
            # print("b0 acquire")

            if self.locked_for_read(key):
                buffer_lock.release()
                return False
            if key not in lock_manager.keys():
                lock_dict = self.table.lock_manager
                lock_dict.add_lock(key, "Read", trans_num)
            # cpage.isPinned += 1
            # print("b0 released")
            buffer_lock.release()
            cpage = self.get_from_disk(key=key)
            if cpage == None:
                return False
            # Get index of record based on key
            rec_index = self.get_rec_ind(cpage, key)
            rec_RID   = cpage.pages[1].retrieve(rec_index)
            if rec_RID == -1:
                return False
            indirection = cpage.pages[0]
            # if record has been updated => grab newest values from tail page
            if rec_RID in indirection.keys():
                # buffer_lock.acquire()
                record = self.get_updated_values(key, query_columns, cpage)
                # buffer_lock.release()
            # Hasnt been updated, grab all values according to query_columns
            else:
                # Gets record from key
                record = self.get_base_values(key, query_columns, cpage)
            if record == None:
                return False
            records.append(record)
            cpage.isPinned -= 1
        else:
            if self.index_exists(column):
                index = self.table.index.indices[column]
                if value in index.keys():
                    val_index = index[value]
                    for rec_val in val_index:
                        rec_ind, path = rec_val
                        record, key = self.get_from_index(rec_ind, path, query_columns)
                        if record == None:
                            return False
                        # if self.locked_for_read(key) and trans_num != lock_manager[key][1]:
                        #     buffer_lock.release()
                        #     return False
                        if key not in lock_manager.keys():
                            lock_dict = self.table.lock_manager
                            lock_dict.add_lock(key, "Read", trans_num)
                        records.append(record)
            else:
                base_paths = self.get_base_paths()
                # Search through entire disk for all base pages
                for path in base_paths:
                    record_vals = self.get_records(path, value, column, query_columns)
                    if record_vals == None:
                        return False
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
        if rec_RID == -1:
            return None
        indirection = cpage.pages[0]
        b_schema    = cpage.pages[3][rec_index]
        tail_path, tail_rec_ind, _ = indirection[rec_RID]
        # buffer_lock.acquire()
        tail_page = self.get_tail_page(cpage, key, True, *[None]*self.table.num_columns)
        # tail_page.isPinned += 1
        # buffer_lock.acquire()
        for i, val in enumerate(b_schema):
            if query_columns[i] == 1:
                if val == 0:
                    # grab from base
                    rec_val = cpage.pages[6+i].retrieve(rec_index)
                    if rec_val == -1:
                        return None
                    else:
                        values.append(rec_val)
                else:
                    # grab from tail
                    rec_val = tail_page.pages[6+i].retrieve(tail_rec_ind)
                    if rec_val == -1:
                        return None
                    else:
                        values.append(rec_val)
            else:
                values.append(None)
        tail_page.isPinned -= 1
        return Record(rec_RID, key, values)

    '''
        Returns record from key
    '''
    def get_base_values(self, key, query_cols, cpage):
        values    = []
        rec_index = self.get_rec_ind(cpage, key)
        base_RID  = cpage.pages[1].retrieve(rec_index)
        if base_RID == -1:
            return None
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
        base_page = self.get_from_disk(path=path, key=None)
        if base_page == None:
            return None, -1
        base_RID    = base_page.pages[1].retrieve(rec_ind)
        key         = base_page.pages[6].retrieve(rec_ind)
        indirection = base_page.pages[0]
        lock_manager = self.table.lock_manager.lock_recs
        if self.locked_for_read(key):
            return None, -1
        if base_RID in indirection.keys():
            record = self.get_updated_values(key, query_columns, base_page)
        else:
            record = self.get_base_values(key, query_columns, base_page)
        base_page.isPinned -= 1
        return record, key

    '''
        Returns records from paths
    '''
    def get_records(self, path, value, column, query_columns):
        records = []
        base_page = self.get_from_disk(path=path, key=None)
        if base_page == None:
            return None
        indirection = base_page.pages[0]
        # for each record in base page
        for rec_ind in range(base_page.num_records):
            rec_RID = base_page.pages[1].retrieve(rec_ind)
            key = base_page.pages[6].retrieve(rec_ind)
            thread_num = self.table.lock_manager.lock_recs[key][1]
            if self.locked_for_read(key) and thread_num != threading.get_ident():
                return None
            if rec_RID in indirection.keys():
                record = self.get_updated_values(key, query_columns, base_page)
                if record == None:
                    return None
                if record.columns[column] == value:
                    records.append(record)
            else:
                record = self.get_base_values(key, query_columns, base_page)
                if record == None:
                    return None
                if record.columns[column] == value:
                    records.append(record)
        base_page.isPinned -= 1
        return records

    ### ******* End of Select Helpers ******* ###

    """
    Update a record with specified key and columns
    Returns True if update is succesful
    Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        buffer_lock.acquire()
        if key not in self.buffer_pool.meta_data.key_dict:
            buffer_lock.release()
            return False
        # get base page & pin -- adds to bufferpool
        lock_manager = self.table.lock_manager.lock_recs
        query_type = "update"
        if self.locked_for_write(query_type, key):
            buffer_lock.release()
            return False
        if key in self.buffer_pool.meta_data.key_dict.keys():
            location = self.buffer_pool.meta_data.key_dict[key]
        else:
            buffer_lock.release()
            return False
        path = './template/%s/'%(self.table.path) +self.table.name+'/PR'+str(location[0])+'/BP'+str(location[1])
        base_page, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'r+b') as db_file:
                base_page = pickle.load(db_file)

            self.buffer_pool.addConceptualPage(base_page)
        base_page.isPinned += 1
        if base_page == None:
            buffer_lock.release()
            return None
        # Get tail page -- adds to bufferpool
        tail_page = self.get_tail_page(base_page, key, False, *columns)
        if tail_page == None:
            buffer_lock.release()
            return False
        success = self.update_tail_page(base_page, tail_page, key, *columns)
        tail_page.dirty = True
        if not success:
            buffer_lock.release()
            return False
        base_page.isPinned -= 1
        base_page.dirty     = True
        if base_page.full():
            if tail_page.path not in self.buffer_pool.merge_tails:
                self.buffer_pool.merge_tails.append(tail_page.path)
            if base_page.path not in self.buffer_pool.merge_bases:
                self.buffer_pool.merge_bases.append(base_page.path)
        self.table.merge_count += 1
        if self.table.merge_count == self.table.merge_frequency:
            threading.Thread(target=self.buffer_pool.merge()).start()
        buffer_lock.release()
        return True

    ### ******* Update Helpers ******* ###

    '''
        Updates a tail record for a new update on a base record
        - On first update for any column, add snapshot with the update
        set pointer in indirection to prev tail record
    '''
    def update_tail_page(self, base_page, tail_page, key, *columns):
        rec_ind          = self.buffer_pool.meta_data.key_dict[key][3]
        base_rec_RID     = base_page.pages[1].retrieve(rec_ind)
        base_indirection = base_page.pages[0]
        new_schema = copy.copy(base_page.pages[3][rec_ind])
        old_schema = base_page.pages[3][rec_ind]
        query_cols = self.update_schema(new_schema, *columns)
        prev_tail_values = []
        tail_rec_ind = tail_page.num_records
        tail_path    = tail_page.path
        tail_schema  = query_cols
        thread_num   = threading.get_ident()
        # check if base record has been updated previously, if it has then grab the values from the previous update.
        if base_rec_RID in base_indirection.keys():
            prev_tail_values = self.get_prev_tail(key, rec_ind, base_rec_RID, base_page, *columns)
        else:
            prev_tail_values = [MAX_INT]*self.table.num_columns
        # If first update for any column, save snapshot
        if not np.array_equal(old_schema, new_schema):
            if tail_page.full():
                tail_page.isPinned -= 1
                pr_num    = base_page.path.split("/")[4][2:]
                tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned +=1
            self.create_snapshot(old_schema, new_schema, rec_ind, base_page, tail_page)
            # if full after creating snapshot
            if tail_page.full():
                tail_page.isPinned -= 1
                pr_num    = base_page.path.split("/")[4][2:]
                tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned +=1
            if not rec_ind in base_indirection.keys():
                base_indir_val = tail_path, tail_rec_ind+1, thread_num
            else:
                base_indir_val = base_indirection[rec_ind]
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, tail_rec_ind, tail_schema, base_indir_val, *columns)
        else: #If we don't create a snapshot
            # If full, create new tail page
            if tail_page.full():
                tail_page.isPinned -= 1
                pr_num    = base_page.path.split("/")[4][2:]
                tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned += 1
            if not rec_ind in base_indirection.keys():
                base_indir_val = tail_path, tail_rec_ind, thread_num
            else:
                base_indir_val = base_indirection[rec_ind]
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, tail_rec_ind, tail_schema, base_indir_val, *columns)

        base_page.pages[3][rec_ind] = new_schema
        tail_rec_ind     = tail_page.num_records - 1
        tail_path        = tail_page.path
        base_page.pages[0][base_rec_RID] = tail_path, tail_rec_ind, threading.get_ident()
        tail_page.isPinned -= 1
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
        # buffer_lock.acquire()
        # makes sure to update RID and tail page record num for the snapshot
        for i, val in enumerate(snapshot_col):
            if val == 1:
                col_val = base_page.pages[i+6].retrieve(rec_ind)
                success = tail_page.pages[i+6].write(col_val)
            else:
                success = tail_page.pages[i+6].write(MAX_INT)
            if not success:
                return False
        tail_page.num_records += 1
        tail_page.dirty = True
        self.buffer_pool.meta_data.tailRID_count += 1
        # buffer_lock.release()
        return True

    '''
        Writes tail record to tail page
    '''
    #tail_schema is query_cols
    def create_tail(self, new_schema, prev_tail_values, tail_page, tail_rec_ind, tail_schema, base_indir_val, *columns):
        # buffer_lock.acquire()
        for i, val in enumerate(new_schema):
            # If page has update at column i
            if val == 1:
                if columns[i] == None:
                    # Set value to prev tail page value
                    success = tail_page.pages[i+6].write(prev_tail_values[i])
                else:
                    # Set value to updated value
                    success = tail_page.pages[i+6].write(columns[i])
            else:
                # Set value to None/MaxInt
                success = tail_page.pages[i+6].write(MAX_INT)
            if not success:
                return None

        # UPDATE schema_col & Indirection Here
        # Set tail_indirection to be old
        tail_page.pages[0][tail_rec_ind] = base_indir_val
        tail_page.pages[3].append(tail_schema)
        tail_page.num_records += 1
        tail_page.dirty = True
        self.buffer_pool.meta_data.tailRID_count += 1
        # buffer_lock.release()
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
        prev_tail_pg     = self.get_tail_page(base_page, key, True, *columns)
        base_rec_RID     = base_page.pages[1].retrieve(rec_ind)
        base_indirection = base_page.pages[0]
        prev_tail_path, prev_tail_rec_ind, _ = base_indirection[base_rid]
        prev_tail_values = []
        for i in range(len(columns)):
            prev_tail_values.append(prev_tail_pg.pages[6+i].retrieve(prev_tail_rec_ind))
        prev_tail_pg.isPinned -= 1
        return prev_tail_values

    ### ******* End of Update Helpers ******* ###

    '''
        Sum all records for the aggregate_column_index between start & end vals
        - returns sum
    '''
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        file_paths = self.get_base_paths() # gets all base page paths
        start_range, end_range = self.start_is_less(start_range, end_range)
        # Check through every base page & their records
        for path in file_paths:
            # get base page from path
            # print("buffer_lock11")
            # buffer_lock.acquire()
            cpage = self.get_from_disk(path=path)
            if cpage == None:
                return None
            # cpage.isPinned += 1
            # buffer_lock.release()
            # check indirection to see if tail page is in indirection
            for i in range(cpage.num_records):
                curr_val = cpage.pages[aggregate_column_index+6].retrieve(i)
                if curr_val == -1:
                    return False
                if self.rec_is_deleted(i, cpage):
                    continue
                if start_range <= curr_val <= end_range:
                    # Add_Sum checks if there is a lock
                    sum_val = self.add_sum(i, aggregate_column_index, cpage)
                    if sum_val == -1:
                        return False
                    sum += sum_val
            cpage.isPinned -= 1
        return sum

    ### ******* Sum Helpers ******* ###
    '''
        Gets all base page paths in disk and buffer_pool
            - Uses regex to match with all base pages when searching in disk/buf
        returns paths
    '''
    def get_base_paths(self):
        regex   = re.compile("./template/%s/%s/PR[0-9]+/BP[0-9]+"%(self.table.path,self.table.name))
        rootdir = './template/%s/'%self.table.path
        file_paths = []
        seen = []
        # Check in disk
        for subdir, dirs, files in os.walk(rootdir, topdown=False):
            for file in files:
                path = os.path.join(subdir, file) # path to file
                # if it matches with some value within the current path, append
                split = path.split('_')
                regex_match = not re.match(regex, path) == None
                if split[0] not in seen and regex_match:
                    seen.append(split[0])
                    file_paths.append(path)
        # Check in buffer_pool
        for path in self.buffer_pool.buffer_keys.keys():
            if not re.match(regex, path) == None and path.split('_')[0] not in seen:
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
        trans_num = threading.get_ident()
        indirection = cpage.pages[0]
        # Check if record is locked on this page
        rec_RID = cpage.pages[1].retrieve(rec_num)
        rec_key = cpage.pages[6].retrieve(rec_num)
        if self.locked_for_read(rec_key):
            return None
        lock_dict = self.table.lock_manager
        lock_dict.add_lock(rec_key, "Read", trans_num)
        # if rec_RID == -1:
        #     return -1
        b_schema    = cpage.pages[3][rec_num]
        # checks if the has indirection & updates been made for that column
        updated = rec_RID in indirection.keys() and b_schema[col_ind] == 1
        if updated:
            tail_path, tail_rec_ind = indirection[rec_RID]
            # print("buffer_lock12")
            # buffer_lock.acquire()
            tail_page = self.get_from_disk(path=tail_path, key=rec_key)
            if tail_page == None:
                return None
            # buffer_lock.release()
            return tail_page.pages[col_ind+6].retrieve(tail_rec_ind)
        else:
            return cpage.pages[col_ind+6].retrieve(rec_num)

    '''
        Checks to see if a record is locked from key by a read operation
            - if trying to access a record with write lock, abort
            - if trying to write to something with read lock, abort
    '''
    def locked_for_read(self, key):
        # lock_manager = [Key:("Write/Read",thread_num]
        lock_manager = self.table.lock_manager.lock_recs
        trans_num    = threading.get_ident()
        # trans_num = self.trans_num
        if key in lock_manager.keys():
            if lock_manager[key][0] == "Write" and trans_num != lock_manager[key][1]:
                return True
            elif key not in self.buffer_pool.meta_data.key_dict.keys():
                return True
            else:
                return False
        # elif key not in self.buffer_pool.meta_data.key_dict.keys():
        #     return True
        else:
            if key not in lock_manager.keys():
                self.table.lock_manager.add_lock(key, "Write", trans_num)
            return False

    '''
        Checks to see if a record is locked from key by a read operation
        - if trying to access a record with write lock, abort
        - if trying to write to something with read lock, abort
        - This function is locked from update
    '''
    def locked_for_write(self, query_type, key):
        lock_manager = self.table.lock_manager.lock_recs
        trans_num = threading.current_thread().ident
        # trans_num = self.trans_num
        if key in lock_manager.keys() and trans_num != lock_manager[key][1]:
            return True
        elif key not in self.buffer_pool.meta_data.key_dict.keys() and query_type != "insert":
            return True
        elif key in self.buffer_pool.meta_data.key_dict.keys():
            rec_ind = self.buffer_pool.meta_data.key_dict[key][-1]
            buffer_lock.release()
            cpage = self.get_from_disk(key=key)
            buffer_lock.acquire()
            if self.rec_is_deleted(rec_ind, cpage):
                return True
        else:
            if key not in lock_manager.keys():
                # print(hex(id(self)))
                # print("locking", trans_num, key)
                # print(lock_manager)
                self.table.lock_manager.add_lock(key, "Write", trans_num)
            # print("\nlocked_for_write: ", trans_num, key, lock_manager, "\n\n")
            return False

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
        indirection  = base_page.pages[0]
        base_rec_ind = self.buffer_pool.meta_data.key_dict[key][3] # record num in bp
        base_rec_RID = base_page.pages[1].retrieve(base_rec_ind)   # RID of base record
        if base_rec_RID == -1:
            return None
        pr_num = base_page.path.split('/')[4] # keep same PR as base page for tail
        # If tail page exists, get values from indirection. Else create new tail path
        if base_rec_RID in indirection.keys():
            tail_path, tail_rec_ind, _ = indirection[base_rec_RID]
        else:
            tail_path = './template/%s/'%(self.table.path) + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
        # if tail not in buffer, get from file and add to buffer_pool. Else just grab from buffer_pool
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            # The first tail page for the table should create a new one
            if not self.buffer_pool.first:
                if self.tail_in_indirection(tail_path, indirection):
                    if key != None:
                        if key not in self.buffer_pool.meta_data.key_dict.keys():
                            return None
                    tail_page, is_in_buffer = self.in_buffer(tail_path)
                    if not is_in_buffer:
                        with open(tail_path, 'rb') as db_file:
                            tail_page = pickle.load(db_file)

                        self.buffer_pool.addConceptualPage(tail_page)
                    tail_page.isPinned += 1

                    if tail_page == None:
                        return None
                    # prev = false for new tail page full & not looking for prev tail values
                    if tail_page.full() and prev == False:
                        tail_page = self.new_tail_page(pr_num, *columns)
                else:
                    tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                    self.add_meta(tail_page)
            else:
                self.buffer_pool.first = False
                tail_page = self.new_tail_page(pr_num, *columns)
        else:
            if tail_page.full() and prev == False:
                tail_page = self.new_tail_page(pr_num, *columns)
        base_page.isPinned -= 1
        return tail_page

    '''
        Gets a page from disk
        - if no path, get path from key
    '''
    def get_from_disk(self, path=None, key=None):
        # If no path, get path for base page from key
        buffer_lock.acquire()
        if path == None:
            if key == None:
                return None
            if key in self.buffer_pool.meta_data.key_dict.keys():
                location = self.buffer_pool.meta_data.key_dict[key]
            else:
                return None
            path = './template/%s/'%(self.table.path) +self.table.name+'/PR'+str(location[0])+'/BP'+str(location[1])
        if key != None:
            if key not in self.buffer_pool.meta_data.key_dict.keys():
                return None
        # if not in buffer, grab from disk
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'r+b') as db_file:
                cpage = pickle.load(db_file)

            self.buffer_pool.addConceptualPage(cpage)
        cpage.isPinned += 1
        buffer_lock.release()
        return cpage

    '''
        Creates new tail page from meta_data path & add to buffer_pool
    '''
    def new_tail_page(self, pr_num, *columns):
        # buffer_lock.acquire()
        tail_path = './template/%s/'%(self.table.path)  + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count // 512))
        # print(tail_path, "\n")
        # tail_page = self.get_from_disk(path=tail_path)
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            # tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
            tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *[None]*self.table.num_columns)
        self.add_meta(tail_page)
        # buffer_lock.release()
        return tail_page

    '''
        Creates a new page range directory in table dir
        Returns path to page range directory
    '''
    def create_pr_dir(self):
        pr_path = './template/%s/'%(self.table.path) +self.table.name+ "/PR"+str(self.buffer_pool.meta_data.currpr)
        os.mkdir(pr_path)  #insert new PR and BP into disk
        return pr_path

    '''
        Checks to see if a cpage is in buffer
        - returns cpage, True if it is // None, False if it isn't
    '''
    def in_buffer(self, path):
        for cpage in self.buffer_pool.conceptual_pages:
            if cpage.path == path:
                #self.buffer_pool.buffer_lock.release()   not sure if needed in insert to bp, could be needed if in buffer called elsewhere
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
        # insert_lock.acquire()
        values = [self.buffer_pool.meta_data.baseRID_count, time_val]
        cpage.pages[1].write(values[0])  # Base_RID col
        cpage.pages[2].write(values[1])  # Timestamp col
        cpage.pages[3].append(np.zeros(len(cpage.pages) - 6)) # Schema
        cpage.pages[4].write(0) # Other Base_RID
        cpage.pages[5].write(0) # TPS col
        # insert_lock.release()

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
