from template.config import *
from template.page import Page
from template.conceptual_page import ConceptualPage
import pickle
import os
import threading
import math
import numpy as np

MAX_INT = int(math.pow(2, 63) - 1)

#we probably want meta data to be stored in the files
class MetaData():
    # data = some tuple of info we can extract below
    def __init__(self):
        self.currpr        = -1 # current page range (-1 bc creation of PR on first insert)
        self.currbp        = 0  # current base page
        self.record_num    = 0  # number of total records
        self.baseRID_count = 0  # number of base records
        self.tailRID_count = 0  # number of tail records
        self.key_dict      = {} # key:(pr#, bp#, page#, rec_ind)

class BufferPool():

    def __init__(self, table_name):
        self.meta_data        = MetaData() # holds table meta data
        self.max_capacity     = 16         # max number of cpages in buffer
        self.conceptual_pages = []         # cpage objects
        self.buffer_keys      = {}         # page_path: page_object
        self.table_name       = table_name # name of table
        self.merge_bases      = []         # holds bp paths ready to merge
        self.merge_tails      = []         # holds tail pages for merge bases
        self.first            = True       # for manual check to make first PR

    '''
        Creates and adds new cpage to buffer_pool
        params: path of new page, bool for is_tail, and column values
                for populate
                - if is_tail, dont populate with column values
        returns the new cpage
    '''
    def createConceptualPage(self, path, is_tail, *columns):
        cpage      = ConceptualPage(columns)
        cpage.path = path
        # dont want to add values to tail
        if not is_tail:
            self.populateConceptualPage(columns, cpage)
        self.addConceptualPage(cpage)
        return cpage

    '''
        Adds a cpage to buffer_pool while following eviction policy
    '''
    def addConceptualPage(self, conceptualPage):
        # evict until space in buffer
        if len(self.conceptual_pages) >= self.max_capacity:
            self.evict()
        self.add_key(conceptualPage) # add page path to buffer_keys
        self.conceptual_pages.append(conceptualPage)

    '''
        Adds columns values to the cpage in parameter
        returns True
    '''
    def populateConceptualPage(self, columns, conceptualPage):
        conceptualPage.num_records += 1
        offset = 6 # number of meta columns
        for i, col in enumerate(columns):
            conceptualPage.pages[i + offset].write(columns[i])
        conceptualPage.pages[3].append(np.zeros(len(columns)))
        return True

    '''
        Evict called when buffer_pool needs to remove a cpage from memory
        - Loops through to find first page not pinnex (waits until finds one)
        - Writes to disk if page is dirty, else just remove from buffer
    '''
    def evict(self):   #evict a physical page from bufferpool (LRU)
        i = 0
        cpage = self.conceptual_pages[i]
        while cpage.isPinned:
            i += 1
            if i == len(self.conceptual_pages):
                i = 0
            cpage = self.conceptual_pages[i]
        cpage = self.conceptual_pages.pop(i) # remove and store unpinned cpage
        cpage.isPinned = False
        self.remove_key(cpage)
        # if changes to write, page is dirty
        if cpage.dirty:
            path = cpage.path
            cpage.dirty = False
            with open(path, 'wb') as db_file:
                pickle.dump(cpage, db_file)

    '''
        Close: evicts all cpages from buffer_pool
    '''
    def close(self):
        while self.conceptual_pages:
            self.evict()

    '''
        Remove_key: Removes a cpage's path from buffer_keys
    '''
    def remove_key(self, conceptual_page):
        del self.buffer_keys[conceptual_page.path]

    '''
        Add_key: Adds a cpage's path to buffer_keys
    '''
    def add_key(self, conceptual_page):
        self.buffer_keys[conceptual_page.path] = conceptual_page

    # # Returns base & tail pages for merging
    # def id_merge_pages(self):
    #     pass

    def get_base_pages(self):
        base_pages = []
        for path in self.merge_bases:
            with open(path, "rb") as db_file:
                base_page = pickle.load(db_file)
                base_pages.append(base_page)

        return base_pages
    def get_tail_pages(self,base_page):
        tail_page_paths = []
        tail_pages = []
        indirection        = base_page.pages[0]
        base_rec_ind       = self.buffer_pool.meta_data.key_dict[key][3] # record num in bp
        time_stamp_column  = base_page.pages[2]
        base_schema_column = base_page.pages[3]
        tps_column         = base_page.pages[4]
        base_RID_column    = base_page.pages[5]
        key_column         = base_page.pages[6]

        # 1.Go thru the indirection and get all tail_paths
        # Add all paths to the tail page that aren't duplicates
        # record = baseRID:(path,tail_RID)
        for records in indirection:
            if records[0] not in tail_pages:
                tail_page_paths.append(records[0])

        # Create tail_page objects here
        for path in tail_page_paths:
            with open(path,"rb") as db_file:
                tail_page_obj = pickle.load(db_file)
                tail_pages.append(tail_page_obj)
        return tail_pages, base_RID_column

    def create_base_copy(self,base_page):
        # Take a
        indirection        = base_page.pages[0]
        tail_page_objs, base_RID_column = get_tail_pages(base_page)
        tail_page_values = []
        new_base_page = copy.copy(base_page)
        for i, key in enumerate(base_RID_column):
            for j, tail_page in tail_page_objs:
                count = 0
                for x in tail_page.pages[0]:
                    if x == key:
                        for k in range(len(base_page.pages[3][0])):
                            new_value = tail_page.pages[k+6].retrieve(count)
                            tail_page.pages[k+6].overWrite(new_base_page.pages[k+6].retrieve(i),new_value)
                        # Here we want the values
                    else:
                        count += 1
        return new_base_page
    def change_dict_vals(self,dictionary):
        pass

    def merge(self):
        possible_merges = []
        to_be_merged = []
        base_pages = self.get_base_pages()
        tail_pages = self.get_tail_pages(base_pages)
        for base_page in base_pages:
            tail_pages = self.get_tail_pages(base_pages)
            create_base_copy(base_page)
        # copy_of_base = self.create_copies()
        # for base_page in base_pages:
        #     for base_record_num in range(512):                                    #should this be 511? starts from 0
        #         base_record_RID = base_page.pages[1].retrieve(base_record_num)
        #         merge_key_dict[base_record_RID] = (base_page, base_record_num)
        #
        # # to check if the base record has already had an update merged with it
        # base_records_already_updated = []
        #
        # # iterate through all tail pages we pulled in
        # for tail_page in tail_pages:
        #     # iterate through all tail records in a given tail page
        #     for tail_record_num in range(511, -1, -1):
        #         # Determine the original base record for this tail record
        #         tail_record_BaseRID = tail_page.pages[5].retrieve(tail_record_num)
        #         # Check if we have already merged an update to this base record.
        #         # If we have merged an update: skip this tail record.
        #         if tail_record_BaseRID in base_records_already_updated:
        #             continue
        #         # grab the record TID and check if it is less than the TPS number
        #         # of the base record: if so, skip this tail record.
        #         tail_record_TID = tail_page.pages[1].retrieve(tail_record_num)
        #         base_page_containing_base_record, base_record_num = merge_RID_dict[tail_record_BaseRID][0], merge_RID_dict[tail_record_BaseRID][1]
        #         base_record_TPS = base_page_containing_base_record.pages[4].retrieve(base_record_num)
        #         if base_record_TPS >= tail_record_TID:
        #             continue
        #         # We can now proceed with updating the values in the base record
        #         # with the appropriate values from the tail record
        #         num_columns = len(tail_page.pages[6:])
        #         offset = 6
        #
        #         for column_num in range(offset, num_columns + offset):
        #             new_value = tail_page.pages[column_num].retrieve(tail_record_num)
        #             # skips to the next column if null
        #             if new_value == MAX_INT:
        #                 continue
        #             else:
        #                 # writes the new value into the base record
        #                 base_page_containing_base_record.pages[column_num].overWrite(base_record_num, new_value)
        #
        # self.merge_bases.clear()  #Clear the merge lists
        # self.merge_tails.clear()
        return True



        # Update 8192 records
        '''
        1.  Identify which tail pages are to be merged:
        Possible methods for this:
            - Tail page records we are merging should be consecutive
            - Preferably use tail page records that are filled
            - Records should be committed
        after 4096 updates
        2. Load corresponding base pages for tail pages
            - Create copies, NOT references to the original base pages
        3. Consolide (actually do the fucking merge)
        4. Update the key_dir in bufferpool metadata to ensure
        '''
        merge_base_pages, merge_tail_pages = self.id_merge_pages()

        merge_RID_dir = copy.copy(self.buffer_pool.meta_data.key_dir)
        merge_RID_dict = {}
        # Identify base pages and tail pages we will be using during merge, add them to the appropriate arrays

        # 2. Make a copy of those base_pages
        # 3. Get tail_pages corresponding to the base_pages you want to merge
            # 3.1 You can do this by using the BaseRID column(Which tells you which tail_page corresponding to the base_page)
        # 4. If you have multiple tail_pages, use the most updated one
        # 5. The base_page_copy's values then becomes the tail_pages value(iterate backwards)
        # 1. Get the base_pages you want to merge (Based off of something)

        self.merge_paths = []
        self.corresponding_tail_pages = [(BP, tail_page_object)]
        # Create a new thread for merge
        # # update at x count
        # merge_t = threading.Thread(target=merge)
        # merge_t.start() # Starts running thread in background
        # ret_val = merge_t.join() # waits until thread is done

        # 1. Get the base_pages you want to merge (Based off of something)
        # 2. Make a copy of those base_pages


# '''
#TODO actually add pages to merge_bases
    def add_base_page(self, base_page):
        self.merge_bases.append(base_page.path)

    def create_copy_of_base(self):
        pass

    # def create_copy_of_base_pages(self, base_pages, num_merges=20):
    #     for i in range(num_merges):
    #         # loop through self.merge_bases
    #         # 1. Add to a new path???
    #         # New directory?
    #         pass
    def update_base_page(self):
        # 1. Grab copy of base_page
        # 2. Get most recent tail_page corresponding to the base
        # BP1, BP2, TP1, TP2, TP3
        # Go to the BP, then the indirection and get the path for ALL the tailpages
        pass


    # Add full base_pages into the array?
    # 3. Get tail_pages corresponding to the base_pages you want to merge
        # 3.1 You can do this by using the BaseRID column(Which tells you which tail_page corresponding to the base_page)

    # def get_tail_pages(self, base_pages):
    #     tail_paths = []
    #     for base_page in base_pages:
    #         # Look at indirection, figure out which tail page
    #         indirection = base_page.pages[0]
    #         for i, record in enumerate(base_page.pages[6]):
    #             key = record.retrieve(i)
    #             if key in indirection.keys():
    #                 tail_paths.append(indirection[key][1])
    #     # Remove duplicate values in tail_paths
    #     tail_paths = list(set(tail_paths))
    #     # Get tail pages off of paths
    #     tail_pages = {}
    #     for path in tail_paths:
    #         with open(path, "rb") as db_file:
    #             tail_page = pickle.load(db_file)
    #             tail_pages[path] = tail_page
    #
    #     return tail_pages

# Cliff merge code vvvv

#         # add base record RIDs to local merge dict
#         for base_page in merge_base_pages:
#             for base_record_num in range(512):
#                 base_record_RID = base_page.pages[1].retrieve(base_record_num)
#                 merge_key_dict[base_record_RID] = (base_page, base_record_num)
#
#         # to check if the base record has already had an update merged with it
#         base_records_already_updated = []
#
#         # iterate through all tail pages we pulled in
#         for tail_page in merge_tail_pages:
#             # iterate through all tail records in a given tail page
#             for tail_record_num in range(511, -1, -1):
#                 # Determine the original base record for this tail record
#                 tail_record_BaseRID = tail_page.pages[5].retrieve(tail_record_num)
#
#                 # Check if we have already merged an update to this base record. If we have merged an update: skip this tail record.
#                 if tail_record_BaseRID in base_records_already_updated:
#                     continue
# 512
# B0
# 1000
#                 # grab the record TID and check if it is less than the TPS number of the base record: if so, skip this tail record.
#                 tail_record_TID = tail_page.pages[1].retrieve(tail_record_num)
#                 base_page_containing_base_record, base_record_num = merge_RID_dict[tail_record_BaseRID][0], merge_RID_dict[tail_record_BaseRID][1]
#                 base_record_TPS = base_page_containing_base_record.pages[4].retrieve(base_record_num)
#                 if base_record_TPS >= tail_record_TID:
#                     continue
#
#                 # We can now proceed with updating the values in the base record with the appropriate values from the tail record
#                 num_columns = len(tail_page.pages[6:])
#                 offset = 6
#
#                 for column_num in range(offset, num_columns + offset):
#                     new_value = tail_page.pages[column_num].retrieve(tail_record_num)
#                     # skips to the next column if null
#                     if new_value == MAX_INT:
#                         continue
#                     else:
#                         # writes the new value into the base record
#                         base_page_containing_base_record.pages[column_num].overWrite(base_record_num, new_value)

        # after consolidating, update key_directory
        # for base_page in merge_base_pages:

                # after consolidating , update key page_directory
        # def update_key_dict(mergedBasePages)
        #     self.buffer_pool.metadata.key_dict[key]= ?...
# '''
