from table import Table, Record
from index import Index
from conceptual_page import ConceptualPage
from page_range import PageRange
from page import Page
from bufferpool import BufferPool
import os
from random import randint
import numpy as np
import time
import math
import pickle

MAX_INT = int(math.pow(2, 63) - 1)
MAX_PAGE_RANGE_SIZE = 8192
MAX_BASE_PAGE_SIZE  = 4096
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

    # """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    # You can tell if a Base Record has been "Deleted" by checking the Indirection
    # column and the Schema encoding column.
    # If the Base record is "Deleted" it will have all zeros in its Schema Encoding
    # column AND STILL point towards a tail record through its indirection column.
    # """
    # def delete(self, key):
    #     # Grab location of base record
    #     # ind = Index(self.table)
    #     # baseR_loc = ind.locate(key, 0, key)[0]
    #     baseR_loc = self.table.key_dict[key]
    #     baseR_p_range, baseR_base_pg, baseR_pg, baseR_rec = baseR_loc
    #     base_pages    = self.table.page_directory[baseR_p_range].range[0][baseR_base_pg].pages
    #     base_rid      = base_pages[1][baseR_pg].retrieve(baseR_rec)
    #     base_schema_i = 512*baseR_pg + baseR_rec
    #     base_schema   = base_pages[3][base_schema_i]
    #     p_range       = self.table.page_directory[baseR_p_range]
    #     # Check indirection column to see if has been updated
    #     indirection = base_pages[0]
    #     updated     = base_rid in indirection.keys()
    #     n_cols      = self.table.num_columns
    #     none_arr    = [None]*n_cols
    #     # If not updated, add tail page with MAX_INT vals and add to indirection
    #     if not updated:
    #         # Update to add tail page with None for all values
    #         self.update(key, *[None]*n_cols)
    #     else:
    #         # Change base schema to all 0's, then update which gives None tail page
    #         base_schema = np.zeros(n_cols)
    #         self.update(key, *[None]*n_cols)
    #
    #     return True

    # def add_meta(self, new_base, page_index, values):
    #     new_base.pages[1][page_index].write(values[0])
    #     new_base.pages[2][page_index].write(values[1])
    #     new_base.pages[3].append(np.zeros(len(new_base.pages) - 4))
    #
    # def new_add_meta(self, path): #path is path to the base page
    #     # Take the path
    #     temp_page = Page()
    #     with open(path+"RID", 'wb') as db_file:
    #         pickle.dump(temp_page, db_file)
    #     with open(path+"TIMESTAMP", 'wb') as db_file:
    #         pickle.dump(temp_page, db_file)
    #     with open(path+"SCHEMA", 'wb') as db_file:
    #         pickle.dump(temp_page, db_file)
    #     i = self.buffer_pool.load(path+"RID")
    #     self.buffer_pool.array[i].write(self.table.RID_count)
    #     self.buffer_pool.array[i].setPath(path+"RID")
    #     i = self.buffer_pool.load(path+"TIMESTAMP")
    #     self.buffer_pool.array[i].write(0)
    #     self.buffer_pool.array[i].setPath(path+"TIMESTAMP")
    #     i = self.buffer_pool.load(path+"SCHEMA")
    #     self.buffer_pool.array[i].write(0)
    #     self.buffer_pool.array[i].setPath(path+"SCHEMA")



    def create_pr_dir(self):
        os.mkdir(os.path.join('ECS165/'+self.table.name, "PR"+str(self.buffer_pool.meta_data.currpr)))  #insert new PR and BP into disk
        # os.mkdir(os.path.join('ECS165/'+self.table.name+"/PR" +str(self.buffer_pool.meta_data.currpr))) # , "BP" + str(self.buffer_pool.meta_data.currbp) ))

    # Checks to see if path is in bufferpool already
    def in_buffer(self, path):
        for cpage in self.buffer_pool.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False

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
        key  = columns[0]
        # if we need to create a new page range
        if new_page_range:
            #keeps track of pr and bp we insert to
            self.buffer_pool.meta_data.currpr += 1
            self.buffer_pool.meta_data.currbp = 0
            self.create_pr_dir()
            path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
            # with new values written in
            self.buffer_pool.createConceptualPage(path, columns)
        else:
            # if need new conceptual page, create in currpr directory
            if new_base_page:
                self.buffer_pool.meta_data.currbp += 1
                path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
                self.buffer_pool.createConceptualPage(path, columns)
            else:
                path = './ECS165/' + self.table.name + '/PR' + str(self.buffer_pool.meta_data.currpr) + '/BP' + str(self.buffer_pool.meta_data.currbp)
                cpage, is_in_buffer = self.in_buffer(path)
                if not is_in_buffer:
                    cpage = self.getBasePage(key, path)
                self.buffer_pool.populateConceptualPage(columns, cpage)
                self.buffer_pool.addConceptualPage(cpage)


        location = (self.buffer_pool.meta_data.currpr, self.buffer_pool.meta_data.currbp, page_index, record_index)
        self.buffer_pool.meta_data.key_dict[key] = location
        self.buffer_pool.meta_data.baseRID_count += 1
        return True

    # """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL (milestone 3)
    # Assume that select will never be called on a key that doesn't exist
    # """

    def select(self, value, column, query_columns):
        if column == 0:
            # get base page (checks if in buffer or not)
            cpage = self.getBasePage(key)
            # Get index of record based on key
            rec_index = 0
            for rec_num in range(cpage.num_records):
                rec_key = cpage.pages[6].retrieve(rec_num)
                if rec_key == key:
                    rec_index = rec_num

            rec_RID = cpage.pages[1].retrieve(rec_index)

            updated = False
            indirection = cpage.pages[0]
            if rec_RID in indirection.keys():
                # DO SHIT WITH UPDATES SHIT HERE
                tail_RID = indirection[rec_RID]
                tail_pg  = self.getTailPage(tail_RID)
                # Get values at schema encoded columns
            else:
                # if base record has not been updated: return base record
                values_to_return = []
                # checks query columns to see which columns we want to return. If 0: return None, else: return value huh
                for i in range(len(query_columns)):
                    if query_columns[i] == 0:
                        values_to_return.append(None)
                    else:
                        values_to_return.append(cpage.pages[i+6].retrieve(rec_index))
                return Record(rec_RID, value, values_to_return)
            # get tail page based on indirection
            # return page
        else:
            # loop through every goddamn base page in the whole damn database
            # for key in self.bufferpool.meta_data.key_dict.keys()

            cpage=self.getBasePage(key)
            rec_index_list=[]
            records_return_list=[]
            rec_index=0

            for rec_num in range(cpage.num_records):
                rec_key = cpage.pages[6+column].retrieve(rec_num)
                if rec_key == key:
                    rec_index = rec_num
                    rec_index_list.append(rec_index)

            for rec_index in rec_index_list:
                rec_RID = cpage.pages[1].retrieve(rec_index)
                updated = False
                indirection = cpage.pages[0]
                if rec_RID in indirection.keys():
                    # DO SHIT WITH UPDATES SHIT HERE
                    tail_RID = indirection[rec_RID]
                    tail_pg  = self.getTailPage(tail_RID)
                    # Get values at schema encoded columns
                else:
                    # if base record has not been updated: return base record
                    values_to_return = []
                    # checks query columns to see which columns we want to return. If 0: return None, else: return value
                    for i in range(len(query_columns)):
                        if query_columns[i] == 0:
                            values_to_return.append(None)
                        else:
                            values_to_return.append(cpage.pages[i+6].retrieve(rec_index))
                            records_return_list.append(Record(rec_RID, value, values_to_return))


            return records_return_list

        # "==== OLD ====="

        # # Grab updated values in tail page
        # if rid in indirection.keys():
        #     tail_rid     = indirection[rid]
        #     tail_page_i  = (tail_rid % 65536) // 4096
        #     page_i       = (tail_rid % 4096) // 512
        #     tail_page    = self.table.page_directory[p_range].range[1][tail_page_i].pages
        #     # for i, col in enumerate(tail_page[4:]):
        #     for i, col in enumerate(tail_page[4:]):
        #         value = col[page_i].retrieve(tail_rid % 512)
        #         if value != MAX_INT:
        #             all_columns[i] = col[page_i].retrieve(tail_rid % 512)
        # # """===== OLD OLD ======"""
        #maintain a key dict in memory, use key dict to check bufferpool for record: if its in bufferpool
        #for key dict right now bp is not correct
        # all_cols = []
        # p_range, base_pg, col_num, index = self.table.key_dict[key]
        # for j in range(len(query_columns)):
        #     path = 'ECS165/'+self.table.name+"/PR"+str(p_range)+ "/BP" + str(base_pg) + '/' + "Column"+ str(j)
        #     i = self.table.bufferpool.checkBuffer(path)   #returns index in bufferpool where page is located, otherwise returns -1
        #     if i>=0: #if it exists in bufferpool
        #         all_cols.append(self.table.bufferpool.array[i].retrieve(index))
        #     else:
        #         i = self.table.bufferpool.load(path)
        #         all_cols.append(self.table.bufferpool.array[i].retrieve(index))
        # rid_path = 'ECS165/'+self.table.name+"/PR"+str(p_range)+ "/BP" + str(base_pg) + '/RID'
        # z = self.table.bufferpool.checkBuffer(rid_path)
        # if z >= 0:
        #     rid = self.table.bufferpool.array[z].retrieve(index)
        # else:
        #     z = self.table.bufferpool.load(rid_path)
        #     rid = self.table.bufferpool.array[z].retrieve(index)
        #
        # columns = []
        # for i, col in enumerate(query_columns):
        #     if col:
        #         columns.append(all_cols[i])
        # rec = Record(rid, key, columns)
        # return [rec]

    # 2. Get the base_page you want to update
    def getBasePage(self, key, path=None):
        if path == None:
            # Is it in the bufferpool
            if key in self.bufferpool.buffer_keys.keys():
                # Get base_page from the buffer
                my_base_page_path = self.bufferpool.buffer_keys[key]
                # Check which base_page has the path
                for cpage in self.bufferpool.conceptual_pages:
                    if cpage.path == my_base_page_path:
                        my_base_page = cpage
                        break
            # Is not in bufferpool, but in DISK
            else:
                my_base_page_location = self.meta_data.key_dict[key]
                table_name = my_base_page_location[0]
                page_range_loc = my_base_page_location[1]
                base_page_loc = my_base_page_location[2]
                # Open file, then return it
                path = './ECS165/' +table_name + '/PR' + page_range_loc + '/BP' + base_page_loc
                with open(path,'rb') as db_file:
                    my_base_page = pickle.load(db_file)
                    # Add to bufferpool
                    # Fetch value from disk and add that value to bufferpool
            self.bufferpool.addConceptualPage(my_base_page)
        else:
            with open(path, 'rb') as db_file:
                my_base_page = pickle.load(db_file)

        return my_base_page



    # 1. Figure out which cols to update to
    def colsToUpdate(self, key, *columns):
        query_colums = []
        for i, col in enumerate(columns):
            if col != None:
                query_columns.append(1)
            else:
                query_columns.append(0)
        return query_columns


    #def create_new_tail(self, columns, path):
        #pass


    def get_tail_page(self, base_page, columns): #

        indirection_column = base_page.pages[0]
        RID_of_base        = base_page.pages[1]
        time_stamp_column  = base_page.pages[2]
        base_schema_column = base_page.pages[3]
        tps_column         = base_page.pages[4]
        base_RID_column    = base_page.pages[5]
        key_column         = base_page.pages[6]




        # No tail page or full
        if indirection_column is None or tail_page.isFull():  #if no tail page or full, create one
            # Create a new tail_page and add to path
            # Get the value of PR, and TP from base_page
            # TPS
            # tail_path = ./ECS165/Table_Name/Page_range#/BP#
            pr_num = base_page.path.split('/')[3]
            tail_path_file = './ECS165/' + self.table.name + pr_num + '/TP' + (self.buffer_pool.meta_data.tailRID_count/512)
            # Create file -- tail in bufferpool
            self.buffer_pool.createConceptualPage(tail_path_file)  #was tail_path
            # Set Indirection column = Key:(tailRID,tailpath)
            indirection_column[RID_of_base] = (self.buffer_pool.meta_data.tailRID_count, tail_path_file)
            # Increment the tailRID_count
            self.buffer_pool.meta_data.tailRID_count += 1
            my_tail_page = self.bufferpool.getBasePage(tail_path)
            # Create a new tail page
            return my_tail_page
        else:
            if RID_of_base in indirection_column.keys():
                tail_path = indirection_column[RID_of_base][1]
                my_tail_page = self.bufferpool.getBasePage(tail_path)
                # Fetch tail_page from the disk or the bufferpool
                #increment tailRID_count?
                self.buffer_pool.meta_data.tailRID_count += 1
                return my_tail_page


    def update_tail_page(self, base_page, tail_page, columns):
        # get a tail_page and update it with columns
        # Fix Schema Encoding
        # Implement snapshot?
        # Update BaseRID
        #STEPS
        #if its this records first update, take a snapshot of the record and put that snapshot into tailpage
            #check schema encoding of basepage, if its all 0s then take a createSnapShot
        base_schema_column = base_page.pages[3]
        snapshot = True
        for val in base_schema_column :  #for each val in schema encoding, check if its 1 ( if 1 its been updated)
            if val == 1:
                snapshot = False
        if snapshot:
            #for each value thats not None in columns, set the tail page at the column to whatevers in bp at the column

        return True

    # """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    # """
    def update(self, key, *columns):
        # """---New---"""
        columns_to_update = self.colsToUpdate(key,columns)
        my_base_page = self.getBasePage(key)
        my_tail_page = self.get_tail_page(my_base_page)
        return self.update_tail_page(my_base_page,my_tail_page,columns)
        # In update_tail_page you MUST INCREMENT THE TAIL_RID_COUNT IN THE BUFFERPOOL -- done in get tail page


        # 3. Get tailPage
        '''
                3. If there is no tail page or full
                3.1 Create a tail page and write it to disk
                3.2 If there is a tail page
                3.2.1 Go to the base_page's indirection col
                Get the path, and also the record num
        '''


        # Get base_page, then (addConceptualPage) check if you need to evict to add to bufferpool
        """---OLD---"""
        location = self.table.key_dict[key] # Assume all keys have been inserted
        query_columns = []
        for i, col in enumerate(columns):
            if col != None:
                query_columns.append(1)
            else:
                query_columns.append(0)

        record = self.select(key, 0, query_columns)
        p_range_loc, b_page_loc, page_loc, record_loc = location

        base_page  = self.table.page_directory[p_range_loc].range[0][b_page_loc].pages

        indirection = base_page[0]
        page_ind    = 512*page_loc + record_loc
        base_schema = base_page[3][page_ind]
        record_rid  = base_page[1][page_loc].retrieve(record_loc)
        cols        = []

        tail_RID = self.table.tail_RID
        prev_tail_RID = tail_RID
        # Base Page stuff
        if record_rid in indirection.keys(): # if update has happened already (ie tail page exists for record)
            tail = indirection[record_rid]
            tail_page_i  = tail // 4096
            page_i       = (tail % 4096) // 512
            record_i     = tail % 512
            # Add updated values to cols
            for i, col in enumerate(self.table.page_directory[p_range_loc].range[1][tail_page_i].pages[4:]):
                if columns[i]==None and not base_schema[i]:
                    cols.append(MAX_INT)
                elif columns[i] == None:
                    cols.append(col[page_i].retrieve(record_i))
                else:
                    cols.append(columns[i])

            prev_tail_RID = tail
        else: # no updates for that record yet
            for col in columns:
                if col != None:
                    cols.append(col)
                else:
                    cols.append(MAX_INT)

        indirection[record_rid] = tail_RID
        self.table.tail_RID += 1

        ## FIGURING OUT WHICH TAIL PAGE TO APPEND TO, CREATE IF DOESNT EXIST
        tail_pages = self.table.page_directory[p_range_loc].range[1]
        p_range = self.table.page_directory[p_range_loc]
        if not tail_pages: # If no tail pages created, create new
            p_range.append_tail_page(ConceptualPage(columns))
        elif tail_pages[-1].full(): # if tail page full, create new
            p_range.append_tail_page(ConceptualPage(columns))
        else:
            tail_pages[-1].pages[3].append(np.zeros(len(columns)))
        # Append to most recent tail page
        # If the last tail page is full
        if tail_pages[-1].num_records % 512 == 0:
            for i, col in enumerate(tail_pages[-1].pages):
                # Not indirection & schema
                if not i == 0 and not i == 3:
                    col.append(Page())
        tail_page_i = tail_pages[-1].num_records // 512
        tail_pages[-1].pages[4][tail_page_i].write(key)
        tail_pages[-1].num_records += 1
        # write column values into new tail page record
        for i, col in enumerate(tail_pages[-1].pages[5:]):
            col[tail_page_i].write(cols[i+1])
        # Update Indirection for tail page
        tail_indirection = tail_pages[-1].pages[0]
        tail_indirection[tail_RID] = prev_tail_RID

        tail_schema = tail_pages[-1].pages[3][-1] # Get most recently added schema encoding
        # update base page schema encoding
        for i, col in enumerate(columns):
            if col != None:
                base_schema[i] = 1
                tail_schema[i] = 1

        return True
    """
    # 1. Figure out which cols to update to
            def colsToUpdate(key, *columns):
# 2. Get the base_page you want to update
            # via the key
            def getBasePage(key, *columns):
3. def getTailPage(key, *columns):
3. If there is no tail page or full
                3.1 Create a tail page and write it to disk
                3.2 If there is a tail page
                3.2.1 Go to the base_page's indirection col
                Get the path, and also the record num

# 4 Get the tailRID of the most recent tail record,
            # and add one, then make the tail_indirection
# 5 If your schema Encoding changes then create
            # snapshot for any values that 0 => 1

    """
# """daniels"""
# def update(self, key, *columns):
#         createSnapShot = False
# def actualUpdate():
#     pass

# def TailPage():
#     # 1. Figure out which cols to update to
#     def colsToUpdate(key, *columns):
#         query_colums = []
#         for i, col in enumerate(columns):
#             if col != None:
#                 query_columns.append(1)
#             else:
#                 query_columns.append(0)
#     # 2. Get the base_page you want to update
#     # via the key
#     def getBasePage(key, *columns):
#         # conceptualPage = basePage
#         conceptualPage = self.bufferpool.read_record(key,columns)
#         record_location = get_record_loc(key,columns)
#         indirection_column = conceptualPage.pages[0]
#         RID_of_base_record = conceptualPage.pages[1]
#         time_stamp_column = conceptualPage.pages[2]
#         base_schema_column = conceptualPage.pages[3]
#         tps_column = conceptualPage.pages[4]
#         base_RID_column = conceptualPage.pages[5]
#         key_column = conceptualPage.pages[6]
#         return conceptualPage
#
#     def getTailPage(key, *columns):
#         my_base_page = getBasePage()
#         """
#         3. If there is no tail page or full
#         3.1 Create a tail page and write it to disk
#         3.2 If there is a tail page
#         3.2.1 Go to the base_page's indirection col
#         Get the path, and also the record num
#         """
#         indirection_column = my_base_page.pages[0]
#         # Gets the RID of a specific record uses it as the key for indirection
#         RID_of_base_record = my_base_page.pages[1].get_record_loc(key)
#         # If the RID is a key of RID:tail_page_path
#         # Case 3.1 No tail Page or tailPage Full
#         if indirection_column[0] is None:
#             tail_path = indirection_column[RID_of_base_record]
#             self.bufferpool.write_tail_to_disk(table,key,columns)
#             my_tail_page=self.bufferpool.read_tail_page(tail_path)
#             return my_tail_page
#         # CASE 3.2 If there is a tail_page
#         if RID_of_base_record in indirection_column.keys():
#             tail_path = indirection_column[RID_of_base_record]
#             my_tail_page = self.bufferpool.read_tail_page(tail_path)
#             return my_tail_page
#
#     # 4 Get the tailRID of the most recent tail record,
#     # and add one, then make the tail_indirection
#     def tailRID_Indirection(key, *columns):
#         my_tail_Page = getTailPage(key,columns)
#         # Need to update lastest RID of tail_page
#         tailRID
#         # Update Schema
#         for i, col in enumerate(columns):
#             if col != None:
#                 base_schema[i] = 1
#                 tail_schema[i] = 1
#     # 5 If your schema Encoding changes then create
#     # snapshot for any values that 0 => 1
#     def createSnapShot(key, *columns):
#     # Get Original value from schema encoding changed and then create a tail record for it
#
#     #1. Figure out which columns we are updating in conceptual Page



    '''
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    '''
    def sum(self, start_range, end_range, aggregate_column_index):
        ind = Index(self.table)
        values = ind.locate_range(start_range, end_range, aggregate_column_index)
        return sum(values)

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
