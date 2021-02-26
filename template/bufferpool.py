from config import *
from page import Page
from conceptual_page import ConceptualPage
import pickle
import os
import threading

#we probably want meta data to be stored in the files
class MetaData():
    # data = some tuple of info we can extract below
    def __init__(self):
        # curr_table & curr_page_range is the currently opened file
        self.currpr = -1
        self.currbp = 0

        self.curr_page_range    = 0
        self.curr_base_range    = 0
        self.curr_physical_page = 0
        self.curr_tail_page     = 0
        self.record_num         = 0
        # last_table & last_page_range keep track of the most recently created table and page_range
        self.last_page_range    = 0
        self.last_base_page     = 0
        self.last_physical_page = 0 # THIS WILL BE THE PATH TO THE
        self.last_record = 0
        # currently opened curr_baseRID and curr_tailRID
        self.baseRID_count = 0
        self.tailRID_count = 0
        # Key:(table,page_range)
        # Might want to make this a file
        self.key_dict = {} #Which file to look at structed key:path
        self.indexes = {} #This is for create_index in index.py
        self.newPageNecessary = False
        # For Inserting Records
        # self.insertion_conceptual_page =
        # self.insertion_conceptual_page_path =

class BufferPool():

    def __init__(self, table_name):
        self.meta_data    = MetaData()
        self.max_capacity = 16
        self.capacity     = 0
        # self.array = [None] * self.max_capacity #array of pages
        # self.array=[]
        self.conceptual_pages = []
        self.buffer_keys = {}
        self.next_evict  = 0
        self.table_name  = table_name
        self.merge_bases = []
        self.merge_tails = []

    def load(self, path):      #loads page associated with path, returns index of bufferpool the loaded page is in
        if self.capacity == self.max_capacity:
            self.evict()
        with open(path, 'rb') as db_file:
            temp_page = pickle.load(db_file)
        for i,value in enumerate(self.array):
            if value == None:
                self.array[i] = temp_page
                self.capacity += 1
                return i

    def createConceptualPage(self, path, columns=None):
        #1. Collect physical pages from array the conceptual page
        my_conceptual_page = ConceptualPage(columns)
        my_conceptual_page.path = path
        if columns != None:
            self.populateConceptualPage(columns, my_conceptual_page) ### TODO:
        self.addConceptualPage(my_conceptual_page)

    def addConceptualPage(self, conceptualPage):
        ### Check if bufferpool is full & evict if necessary ###
        # Check if conceptual_pages length > limit
        if len(self.conceptual_pages) >= self.max_capacity:
            self.evict()
        self.add_keys(conceptualPage)
        self.conceptual_pages.append(conceptualPage)

    # Adds new record values to a conceptual page
    def populateConceptualPage(self, columns, conceptualPage):
        #
        ### TODO: Make work with values instead of record ###
        offset = 6
        for i in range(0, len(columns)):
            conceptualPage.pages[i + offset].write(columns[i])
        return True
        # conceptualPage.pages[1].write(record.rid)
        # conceptualPage.pages[2].write(record.time)
        # conceptualPage.pages[3].append(np.zeros(len(base_page.pages) - 6))
        # conceptualPage.pages[4].write(record.TPS)
        # conceptualPage.pages[5].write(record.baseRID)
        # for i, col in enumerate(record.columns):
        #     conceptualPage.pages[i+6].write(col)
        #
        # self.num_records += 1
        pass
    ### Assuming pages stack will be LRU at top of stack (index 0)
    def evict(self):   #evict a physical page from bufferpool (LRU)
        ### check if value being evicted is pinned
        # Write to disk whatever is at the top of stack
        temp_cpage = self.conceptual_pages.pop(0)
        self.remove_keys(temp_cpage)
        with open(temp_cpage.path, 'wb') as db_file:
            pickle.dump(temp_cpage, db_file)
        # TypeError: file must have a 'write' attribute

    #def commit(self):  #commit changes in bufferpool to memory

    def checkBuffer(self,path):  #given a path to disk, check if that page is already in bufferpool
        for i,value in enumerate(self.array):
            if value:
                if value.path == path:
                    return i
        return -1

    def close(self):# evict everything from bufferpool
        for i,value in enumerate(self.array):
            if value:
                with open(value.path, 'wb') as db_file:
                    pickle.dump(value,db_file)


    def remove_keys(self, conceptual_page):
        for i in range(conceptual_page.num_records):
            key_i = conceptual_page.pages[6].retrieve(i)
            del self.buffer_keys[key_i]

    def add_keys(self, conceptual_page):
        for i in range(conceptual_page.num_records):
            key_i = conceptual_page.pages[6].retrieve(i)
            self.buffer_keys[key_i] = conceptual_page

    # Returns base & tail pages for merging
    def id_merge_pages(self):
        pass
# '''
    def add_base_page(self, base_page):
        self.merge_bases.append(base_page.path)
        pass
    def create_copy_of_base(self):
        pass

    def create_copy_of_base_pages(self, num_merges=20, base_pages):
        for i in range(num_merges):
            # loop through self.merge_bases
            # 1. Add to a new path???
            # New directory?
            pass
    def get_tail_pages(self):
        # Go to
        pass
    def update_base_page(self):
        # 1. Grab copy of base_page
        # 2. Get most recent tail_page corresponding to the base
        # BP1, BP2, TP1, TP2, TP3
        # Go to the BP, then the indirection and get the path for ALL the tailpages
        pass


    # Add full base_pages into the array?
    # 3. Get tail_pages corresponding to the base_pages you want to merge
        # 3.1 You can do this by using the BaseRID column(Which tells you which tail_page corresponding to the base_page)
    def identify_pages_to_merge(self):

        #since tail and base pages are in the same folder
        # we have to loop through every conceptual page and check if it's a base page
        #then loop through schema encoding and see if it needs to be merged
        self.add_base_page(base_page)

        #if conceptual_page.pages[0]
        #check through all  base pages and  check if
        merge_base_pages=[]
        merge_tail_pages=[]
        # for conceptual_page  in # BP directory :
        #     #look at schema encoding column
        #     for i in range(0,8):
        #         if conceptual_page.pages[3].retrieve(i)== 1 :
        #
        #             if conceptual_page in merge_base_pages:
        #                 continue
        #             else:
        #                 merge_base_pages.append(conceptual_page)
        #             #find tail pages associated w/ this  base pages
        #             # ignore the tail page if we already have it in our list
        #             #otherwise append
        #             #PLACEHOLDER CODE
        #             tailRID, tail_page_path = conceptual_page.pages[0].retrieve()
        #             tail_page = tail_page_path[0] #?
        #             if tail_page in merge_tail_pages:
        #                 continue
        #             else:
        #                 merge_tail_pages.append(tail_page)
        #     return merge_base_pages, merge_tail_pages

    def get_tail_pages(self, base_pages):
        tail_paths = []
        for base_page in base_pages:
            # Look at indirection, figure out which tail page
            indirection = base_page.pages[0]
            for i, record in enumerate(base_page.pages[6]):
                key = record.retrieve(i)
                if key in indirection.keys():
                    tail_paths.append(indirection[key][1])
        # Remove duplicate values in tail_paths
        unique_t_paths = []
        [unique_t_paths.append(path) for path in tail_paths if path not in res]
        # Get tail pages off of paths
        tail_pages = {}
        for path in unique_t_paths:
            with open(path, "rb") as db_file:
                tail_page = pickle.load(db_file)
                tail_pages[path] = tail_page

        return tail_pages


    def merge_pages(self, base_pages, tail_pages):
        # add base record RIDs to local merge dict




    def get_base_pages(self):
        base_pages = []
        for path in self.merge_bases:
            with open(path, "rb") as db_file:
                base_page = pickle.load(db_file)
                base_pages.append(base_page)

        return base_pages

    def merge(self):
        possible_merges = []
        base_pages = self.get_base_pages()
        tail_pages = self.get_tail_pages(base_pages)

        # copy_of_base = self.create_copies()
        for base_page in base_pages:
            for base_record_num in range(512):
                base_record_RID = base_page.pages[1].retrieve(base_record_num)
                merge_key_dict[base_record_RID] = (base_page, base_record_num)

        # to check if the base record has already had an update merged with it
        base_records_already_updated = []

        # iterate through all tail pages we pulled in
        for tail_page in tail_pages:
            # iterate through all tail records in a given tail page
            for tail_record_num in range(511, -1, -1):
                # Determine the original base record for this tail record
                tail_record_BaseRID = tail_page.pages[5].retrieve(tail_record_num)
                # Check if we have already merged an update to this base record.
                # If we have merged an update: skip this tail record.
                if tail_record_BaseRID in base_records_already_updated:
                    continue
                # grab the record TID and check if it is less than the TPS number
                # of the base record: if so, skip this tail record.
                tail_record_TID = tail_page.pages[1].retrieve(tail_record_num)
                base_page_containing_base_record, base_record_num = merge_RID_dict[tail_record_BaseRID][0], merge_RID_dict[tail_record_BaseRID][1]
                base_record_TPS = base_page_containing_base_record.pages[4].retrieve(base_record_num)
                if base_record_TPS >= tail_record_TID:
                    continue
                # We can now proceed with updating the values in the base record
                # with the appropriate values from the tail record
                num_columns = len(tail_page.pages[6:])
                offset = 6

                for column_num in range(offset, num_columns + offset):
                    new_value = tail_page.pages[column_num].retrieve(tail_record_num)
                    # skips to the next column if null
                    if new_value == MAX_INT:
                        continue
                    else:
                        # writes the new value into the base record
                        base_page_containing_base_record.pages[column_num].overWrite(base_record_num, new_value)





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




        #key:rid value:path to tail page
        #indirec colum gives tail rid -> figure out which tail page
        #key -> tuple->path  to tail page



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
