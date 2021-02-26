from table import Table, Record
from index import Index
from conceptual_page import ConceptualPage
from page_range import PageRange
from page import Page
from bufferpool import BufferPool

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
        self.table = table
        self.buffer_pool = BufferPool(self.table.name)

    """
    internal Method
    Read a record with specified RID
    Returns True upon succesful deletion
    Return False if record doesn't exist or is locked due to 2PL
    You can tell if a Base Record has been "Deleted" by checking the Indirection
    column and the Schema encoding column.
    If the Base record is "Deleted" it will have all zeros in its Schema Encoding
    column AND STILL point towards a tail record through its indirection column.
    """
    def delete(self, key):
        # Grab location of base record
        # ind = Index(self.table)
        # baseR_loc = ind.locate(key, 0, key)[0]
        baseR_loc = self.table.key_dict[key]
        baseR_p_range, baseR_base_pg, baseR_pg, baseR_rec = baseR_loc
        base_pages    = self.table.page_directory[baseR_p_range].range[0][baseR_base_pg].pages
        base_rid      = base_pages[1][baseR_pg].retrieve(baseR_rec)
        base_schema_i = 512*baseR_pg + baseR_rec
        base_schema   = base_pages[3][base_schema_i]
        p_range       = self.table.page_directory[baseR_p_range]
        # Check indirection column to see if has been updated
        indirection = base_pages[0]
        updated     = base_rid in indirection.keys()
        n_cols      = self.table.num_columns
        none_arr    = [None]*n_cols
        # If not updated, add tail page with MAX_INT vals and add to indirection
        if not updated:
            # Update to add tail page with None for all values
            self.update(key, *[None]*n_cols)
        else:
            # Change base schema to all 0's, then update which gives None tail page
            base_schema = np.zeros(n_cols)
            self.update(key, *[None]*n_cols)

        return True

    def create_pr_dir(self):
        os.mkdir(os.path.join('ECS165/'+self.table.name, "PR"+str(self.buffer_pool.meta_data.currpr)))  #insert new PR and BP into disk

    # Checks to see if path is in bufferpool already
    def in_buffer(self, path):
        for cpage in self.buffer_pool.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False

    def add_meta(self, cpage):
        # Get current time value
        current_time = datetime.now().time()
        time_val     = ""
        hour = 0
        # Extract hour * 60, then add to minutes to get total current time in minutes
        for digit in current_time.strftime("%H:%M"):
            if not digit == ":":
                time_val = time_val + digit
            else:
                hour = int(time_val) * 60
                time_val = ""
        time_val = int(time_val) + hour
        values = [self.buffer_pool.meta_data.baseRID_count, time_val]

        cpage.pages[1].write(values[0])
        cpage.pages[2].write(values[1])
        cpage.pages[3].append(np.zeros(len(cpage.pages) - 6))
        cpage.pages[4].write(0)
        cpage.pages[5].write(0)

    # """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    # """
    # Tuple of columns(Key,Value)
    def insert(self, *columns):
        new_page_range = self.buffer_pool.meta_data.baseRID_count  % MAX_PAGE_RANGE_SIZE == 0
        new_base_page  = self.buffer_pool.meta_data.baseRID_count  % MAX_PHYS_PAGE_SIZE  == 0
        new_page       = self.buffer_pool.meta_data.baseRID_count  % MAX_PHYS_PAGE_SIZE  == 0
        page_index     = (self.buffer_pool.meta_data.baseRID_count % MAX_BASE_PAGE_SIZE) // MAX_PHYS_PAGE_SIZE
        record_index   = self.buffer_pool.meta_data.baseRID_count  % MAX_PHYS_PAGE_SIZE
        key            = columns[0]
        # if we need to create a new page range
        if new_page_range:
            #keeps track of pr and bp we insert to
            self.buffer_pool.meta_data.currpr += 1
            self.buffer_pool.meta_data.currbp = 0
            self.create_pr_dir()
            path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
            # with new values written in
            cpage = self.buffer_pool.createConceptualPage(path, *columns)

        else:
            # if need new conceptual page, create in currpr directory
            if new_base_page:
                self.buffer_pool.meta_data.currbp += 1
                path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
                cpage = self.buffer_pool.createConceptualPage(path, *columns)
            else:
                path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
                cpage, is_in_buffer = self.in_buffer(path)
                if not is_in_buffer:
                    cpage = self.get_from_disk(key=key, path=path)
                    self.buffer_pool.addConceptualPage(cpage)

                self.buffer_pool.populateConceptualPage(columns, cpage)

        self.add_meta(cpage)
        location = (self.buffer_pool.meta_data.currpr, self.buffer_pool.meta_data.currbp, page_index, record_index)
        self.buffer_pool.meta_data.key_dict[key] = location
        self.buffer_pool.meta_data.baseRID_count += 1
        return True


    def get_rec_ind(self, cpage, key):
        for rec_num in range(cpage.num_records):
            rec_key = cpage.pages[6].retrieve(rec_num)
            if rec_key == key:
                return rec_num

    # """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL (milestone 3)
    # Assume that select will never be called on a key that doesn't exist
    # """

    def select(self, value, column, query_columns):
        values  = [] # (rec_RID, key, columns) [1, 1, 0, 0] -> [val, val, None, None]
        records = []
        if column == 0:
            # get base page (checks if in buffer or not)
            key   = value
            cpage = self.get_from_disk(key=key)
            # Get index of record based on key
            rec_index = self.get_rec_ind(cpage, key)
            rec_RID   = cpage.pages[1].retrieve(rec_index)
            indirection = cpage.pages[0]
            b_schema    = cpage.pages[3][rec_index]

            # if record has been updated => grab newest values from tail page
            if rec_RID in indirection.keys():
                columns = [None] * self.table.num_columns
                tail_path, tail_rec_ind = indirection[rec_RID]
                tail_page = self.get_tail_page(cpage, key, *columns)
                # 92106429, 2#2, 12, 2, 1
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
            # Hasnt been updated, grab all values according to query_columns
            else:
                for i, val in enumerate(query_columns):
                    if val == 1:
                        values.append(cpage.pages[6+i].retrieve(rec_index))
                    else:
                        values.append(None)
            records.append(Record(rec_RID, key, values))
            # original: key, 7, 6, 2, 10
            # base: key, 15???, 6, 2, 10 -- Should be same as original
            # tail: max, 2, max, max, max -- I guess tail should be: key/max, 7, 14, max, max (makes me think just wrong tail page -- indirection?)
            # schema = [0, 1, 0, 0, 0]    -- schema should be [0, 1, 1, 0, 0] -- so maybe also not updating schema properly?
            # records[0].columns = key, 2, 6, 1, 10 -- correct = key, 7, 14, 2, 10 ?????????
            ### -- either updating tail wrong or getting wrong tail
            # ---- ^^ updated_columns = [None, None, 14, None, None]
        return records

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


    # 2. Get the base_page you want to update
    def get_from_disk(self, key=None, path=None):
        if path == None:
            # Is it in the bufferpool'
            location = self.buffer_pool.meta_data.key_dict[key]
            path     = './ECS165/'+self.table.name+'/PR'+str(location[0])+'/BP'+str(location[1])
            # Check which base_page has the path
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'rb') as db_file:
                cpage = pickle.load(db_file)
        return cpage



    # 1. Figure out which cols to update to
    def colsToUpdate(self, key, *columns):
        query_columns = []
        for i, col in enumerate(columns):
            if col != None:
                query_columns.append(1)
            else:
                query_columns.append(0)
        return query_columns


    #def create_new_tail(self, columns, path):
        #pass

    def tail_in_indirection(self, tail_path, indirection):
        for val in indirection.values():
            if tail_path == val[0]:
                return True

        return False


    def get_tail_page(self, base_page, key, *columns):
        indirection        = base_page.pages[0]
        RID_of_base        = self.buffer_pool.meta_data.key_dict[key][3] # record num in bp
        time_stamp_column  = base_page.pages[2]
        base_schema_column = base_page.pages[3]
        tps_column         = base_page.pages[4]
        base_RID_column    = base_page.pages[5]
        key_column         = base_page.pages[6]
        # No tail page or full
        pr_num = base_page.path.split('/')[3]
        # ./ECS165/Grades/PRO/TP0
        tail_path = './ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
        # './ECS165/Grades/PR0/TP0'
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            if not self.buffer_pool.first:
                if self.tail_in_indirection(tail_path, indirection):
                    with open(tail_path, "rb") as db_file:
                        tail_file = pickle.load(db_file)
                        if tail_file.full():
                            tail_page = self.buffer_pool.createConceptualPage(tail_path, *columns)
                            self.add_meta(tail_page)
                        else:
                            tail_page = tail_file
                else:
                    tail_page = self.buffer_pool.createConceptualPage(tail_path, *columns)
                    self.add_meta(tail_page)
            else:
                # Not the first page
                self.buffer_pool.first = False
                tail_page = self.buffer_pool.createConceptualPage(tail_path, *columns)
                self.add_meta(tail_page)
        else:
            if tail_page.full():
                tail_page = self.buffer_pool.createConceptualPage(tail_path, *columns)
                self.add_meta(tail_page)
        return tail_page

    def update_schema(self, base_schema, query_cols):
        for i, col in enumerate(query_cols):
            if col == 1:
                base_schema[i] = 1

    def update_tail_page(self, base_page, tail_page, key, *columns):
        record_index = self.buffer_pool.meta_data.key_dict[key][3]
        # base_rec_RID     = base_page.pages[1].retrieve(record_index)
        base_indirection = base_page.pages[0]
        new_schema = copy.copy(base_page.pages[3][record_index])
        old_schema = base_page.pages[3][record_index]
        query_cols = self.colsToUpdate(key, *columns)
        # Adds record to tail_page
        self.update_schema(new_schema, query_cols)
        # Old:[0,0,1,0,0]
        # New:[0,1,1,0,0]
        # Snapshot_col = [0,1,0,0,0]
        if old_schema.all() != new_schema.all():
            self.buffer_pool.meta_data.tailRID_count += 1
            snapshot_col = []
            # Goes thru to check which schema differed from the last
            for i in range(len(old_schema)):
                if new_schema[i] != old_schema[i]:
                    snapshot_col.append(1)
                else:
                    snapshot_col.append(0)
            # Create the snapshot HERE
            for i, val in enumerate(snapshot_col):
                if val == 1:
                    tail_page.pages[i+6].write(base_page.pages[i+6].retrieve(record_index))
                else:
                    tail_page.pages[i+6].write(MAX_INT)

            # Create tailpage HERE
            for i, val in enumerate(query_cols):
                if val == 1:
                    tail_page.pages[i+6].write(columns[i])
                # else:
                #     tail_page.pages[i+6].write(MAX_INT)
            # UPDATE schema_col
            # base_page.pages[3][record_index] = new_schema
        else: #If we don't create a snapshot
            # Create tailpage HERE
            for i, val in enumerate(query_cols):
                if val == 1:
                    tail_page.pages[i+6].write(columns[i])
                # else:
                #     tail_page.pages[i+6].write(MAX_INT)

        base_page.pages[3][record_index] = new_schema

        self.buffer_pool.meta_data.tailRID_count += 1
        tail_rec_ind = tail_page.num_records - 1
        tail_path    = tail_page.path
        base_rec_RID     = base_page.pages[1].retrieve(record_index)
        base_page.pages[0][base_rec_RID] = tail_path, tail_rec_ind
        # base_indirection[base_rec_RID] = tail_path, tail_rec_ind
        return True

    # def update_tail_page(self, base_page, tail_page, key, *columns):
    #     #if its this records first update, take a snapshot of the record and put that snapshot into tailpage
    #         #check schema encoding of basepage, if its all 0s then take a createSnapShot
    #     record_index       = self.buffer_pool.meta_data.key_dict[key][3]
    #     base_schema_column = base_page.pages[3][record_index]
    #     base_indirection   = base_page.pages[0]
    #     base_rec_RID       = base_page.pages[1].retrieve(record_index)
    #     snapshot           = False
    #     snap_vals          = []
    #     # Get query_columns
    #     # query_cols = self.colsToUpdate(key, *columns)
    #     # # Get record_index and tail_page, update to it
    #     # for i, col in enumerate(columns):
    #     #     tail_page.pages[i+6].write(columns[i])
    #     # Update the schema encoding
    #     # Create snapshot
    #     for i, col in enumerate(columns):
    #         # If something exist and
    #         # if col !=None and base_schema_column[i] == 0
    #         # Create tail record first, check schema column
    #         # Want to create_snapshot if old_schema != new_schema
    #         if col != None and base_schema_column[i] == 0:
    #             snap_vals.append(1)
    #             snapshot = True
    #         else:
    #             snap_vals.append(0)
    #
    #     query_cols = self.colsToUpdate(key, *columns)
    #
    #     if snapshot:
    #         self.buffer_pool.meta_data.tailRID_count += 1
    #         #for each value thats not None in columns, set the tail page at the column to snapshot of BP
    #         # query_cols = [0,1,0,1,0]
    #         for i,val in enumerate(query_cols):
    #             if val == 1:
    #                 # Creates a new record from old
    #                 tail_page.pages[i+6].write(base_page.pages[i+6].retrieve(record_index))
    #             else:
    #                 tail_page.pages[i+6].write(MAX_INT)
    #
    #         self.update_schema(base_schema_column, query_cols)
    #         base_page.pages[3][record_index] = query_cols  # update schema encoding
    #         #now create another tail record
    #         tail_page = self.get_tail_page(base_page, key, *columns)
    #         for i, val in enumerate(query_cols):
    #             if val == 1:
    #                 tail_page.pages[i+6].write(columns[i])
    #             # else:
    #             #     tail_page.pages[i+6].write(MAX_INT)
    #     else:
    #         for i, val in enumerate(query_cols):
    #             if val == 1:
    #                 tail_page.pages[i+6].write(columns[i])
    #             # else:
    #             #     tail_page.pagues[i+6].write(MAX_INT)
    #         self.update_schema(base_schema_column, query_cols)
    #
    #     self.buffer_pool.meta_data.tailRID_count += 1
    #     tail_rec_ind = tail_page.num_records - 1
    #     tail_path    = tail_page.path
    #     base_indirection[base_rec_RID] = tail_path, tail_rec_ind
    #     return True

    # """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    # """
    def update(self, key, *columns):
        # """---New---"""
        # columns_to_update = self.colsToUpdate(key, *columns)
        my_base_page = self.get_from_disk(key=key)
        tail_page = self.get_tail_page(my_base_page, key, *columns)
        return self.update_tail_page(my_base_page, tail_page, key, *columns)

    def page_is_deleted(self, b_schema, indirection, rec_RID):
        for val in b_schema:
            if val == 1:
                return False
        if rec_RID in indirection.keys():
            return True
        return False
    '''
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Used through the bufferpool
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    '''
    def sum(self, start_range, end_range, aggregate_column_index):
        #1. Get start_range to end_ranges RIDs from from base_pages
        # self.buffer_pool.
        # Iterate through all files with BP and collect all paths
        #2. Get sum from a certain column
        # path = "./ECS165/%s/PR%s/BP%s" % (self.table.name, pr_num, bp_num)
        regex = re.compile("./ECS165/%s/PR[0-9]+/BP[0-9]+"%self.table.name)
        '''
        match = re.match(regex, path)
        match.span # indices that match in target string
        match.match # the string that matched regex exp
        if not re.match(regex, path) == None:
            pass
        '''
        rootdir = './ECS165/'
        # Look
        sum = 0
        file_paths = []
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                path = os.path.join(subdir, file)
                if not re.match(regex, path) == None:
                    file_paths.append(path)

        started  = False
        finished = False
        for path in file_paths:
            # get base page from path
            cpage = self.get_from_disk(path=path)
            cpage.isPinned = True

            _, is_in_buffer = self.in_buffer(path)
            if not is_in_buffer:
                self.buffer_pool.addConceptualPage(cpage)
            rid_col = cpage.pages[1]
            indirection = cpage.pages[0]
            # check indirection to see if tail page is in indirection
            for i in range(cpage.num_records):
                # if record deleted: Continue
                key     = cpage.pages[6].retrieve(i)
                rec_RID = cpage.pages[1].retrieve(i)
                if key == end_range:
                    finished = True
                elif key == start_range:
                    started = True
                b_schema = cpage.pages[3][i]
                # check if record has been deleted, if deleted then skip to the next record
                if self.page_is_deleted(b_schema, indirection, rec_RID):
                    continue

                if rec_RID in indirection.keys() and b_schema[aggregate_column_index] == 1:
                    columns = [None] * self.table.num_columns
                    tail_page = self.get_tail_page(cpage, key, *columns)
                    tail_path, tail_rec_ind = indirection[rec_RID]

                    sum += tail_page.pages[aggregate_column_index+6].retrieve(tail_rec_ind)
                    if started and finished:
                        cpage.isPinned = False
                        return sum
                else:
                    sum += cpage.pages[aggregate_column_index+6].retrieve(i)
                    if started and finished:
                        cpage.isPinned = False
                        return sum

            cpage.isPinned = False
        return sum

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
