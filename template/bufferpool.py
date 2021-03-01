from template.config import *
from template.page import Page
from template.conceptual_page import ConceptualPage
import math, threading, os, pickle, copy
import numpy as np

MAX_INT = int(math.pow(2, 63) - 1)

#we probably want meta data to be stored in the files
class MetaData():
    # data = some tuple of info we can extract below
    def __init__(self):
        self.currpr          = -1    # current page range (-1 bc creation of PR on first insert)
        self.currbp          = 0     # current base page
        self.record_num      = 0     # number of total records
        self.baseRID_count   = 0     # number of base records
        self.tailRID_count   = 0     # number of tail records
        self.key_dict        = {}    # key:(pr#, bp#, merge_number, rec_ind)
        self.key_dict_locked = False # When merging lock to prevent insert changes

class BufferPool():

    def __init__(self, table_name, num_columns):
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
            base_page, is_in_buffer = self.in_buffer(path)
            if not is_in_buffer:
                with open(path, "rb") as db_file:
                    base_page = pickle.load(db_file)
            base_pages.append(base_page)

        return base_pages

    def get_tail_pages(self, base_page):
        # tail_page_paths = []
        # tail_pages = []
        # indirection         = base_page.pages[0]
        # base_rec_ind        = self.meta_data.key_dict[key][3] # record num in bp
        # base_rec_RID_column = base_page.pages[1]
        # time_stamp_column   = base_page.pages[2]
        # base_schema_column  = base_page.pages[3]
        # tps_column          = base_page.pages[4]
        # baseRID_column      = base_page.pages[5]
        # key_column          = base_page.pages[6]
        # 1.Go thru the indirection and get all tail_paths
        # Add all paths to the tail page that aren't duplicates
        # record = baseRID:(path,tail_RID)
        # for records in indirection:
        #     if records[0] not in tail_pages:
        #         tail_page_paths.append(records[0])
        # Create tail_page objects here
        tail_pages = []
        for path in self.merge_tails:
            tail_page, is_in_buffer = self.in_buffer(path)
            if not is_in_buffer:
                with open(path, "rb") as db_file:
                    tail_page = pickle.load(db_file)
            tail_pages.append(tail_page)
        return tail_pages

    def create_base_copy(self, base_page, tail_pages):
        new_base_page  = copy.deepcopy(base_page)
        indirection    = new_base_page.pages[0]
        tail_page_objs = tail_pages
        tail_page_values = []
        new_base_page.pages[1]
        baseRID:(tail_path,tail_RID)
        # go through each
        for base_rec_num in range(new_base_page.num_records):
            base_rec_RID = new_base_page.pages[1].retrieve(base_rec_num)
            # if not in indirection, we don't need to check for updates because it hasn't been updated
            if not base_rec_RID in indirection.keys():
                continue
            tail_page_path, tail_rec_ind = indirection[base_rec_RID]
            # iterate through list of tail pages
            for tail_page in tail_page_objs:
                # check if the tail page were looking at in the list of tail pages is the one containing the latest tail record for this base record
                if tail_page_path == tail_page.path:
                    # go through schema encoding of base record
                    for k in range(len(base_page.pages[3][base_rec_num])):
                        # if schema encoding 0: keep original base record value for that column
                        if base_page.pages[3][base_rec_num][k] == 0:
                            pass
                        # if schema encoding 1: grab new value from tail page and overwrite for that column
                        else:
                            new_value = tail_page.pages[k+6].retrieve(tail_rec_ind)
                            new_base_page.pages[k+6].overWrite(new_value, base_rec_num)

        return new_base_page

    def set_new_path(self,base_page,merge_num):
        # Path = ./ECS165/Grades/PR#/BP#_M#
        new_path = str(base_page.path) + '_M' + str(base_page.merge_num)
        base_page.path = new_path
        base_page.merge_num += 1
        with open(new_path,'wb') as db_file:
            pickle.dump(base_page, db_file)
        return new_path

    '''
        For each key in consolidated base page, change key path to be
        new base page in merge
    '''
    def change_dict_vals(self, base_page, new_base_path):
        # Take the dictionary, and iterate through it for KEYS
        # If the key is the same as the key in the base_page, then replace the key's value with the new one
        # Redefine keys to map to keys:new_base_path
        # Location Tuple(PR, BP, M, Record_Num)]
        # Abstract out the numbers from the file_string
        # Path = ./ECS165/Grades/PR#/BP#_M#
        #path.split('/') = ['.', 'ECS165', 'Grades', 'PR1', 'BP2_M4']
        path = base_page.path
        pr_num = path.split('/')[4][2:]
        bp_str = path.split('/')[5].split('_')
        bp_num = bp_str[0][2:]
        m_num  = base_page.merge_num

        for rec_ind in range(base_page.num_records):
            record_key = base_page.pages[6].retrieve(rec_ind)
            new_loc = (pr_num, bp_num, m_num, rec_ind)
            self.meta_data.key_dict[record_key] = new_loc

        # while count != 512:
        #     for key_in_dict in self.key_dict:
        #         if key_in_dict == base_page.pages[6].retrieve(count):
        #             # set the key to new_path
        #             self.key_dict.set(key_in_dict, new_loc)
        #             count += 1

    '''
        - No limit to memory for merge
        - Get Base pages that are ready for merge
            - Get corresponding tail pages
        - Create copy of current base page
        - Merge Tail Pages with base page copy
        - Create new base page path
    '''
    def merge(self):
        # 1. Get a base_page from the set of possible merges
        # 2. Get the corresponding tail_pages
        # 3. Create a new copy of the base_page
        # 4. Merge the tail_pages with the new base_page
        # 5. Create a new file called # Path = ./ECS165/Grades/PR#/BP#_M#
        # 6. Go through the key_dict and find all the Keys that are equalivent to the keys in the new_base_page and replace path
        possible_merges = []
        # to_be_merged = self.merge_bases
        base_pages = self.get_base_pages()
        # tail_pages = self.get_tail_pages(base_pages)
        base_pages = self.get_base_pages()
        for base_page in base_pages:
            merge_num = base_page.merge_num
            tail_pages = self.get_tail_pages(base_page)
            new_base = self.create_base_copy(base_page, tail_pages)
            new_base_path = self.set_new_path(new_base, merge_num)
            self.change_dict_vals(new_base, new_base_path)
            base_page.merge_num += 1
        return
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
        # return True

    '''
        Checks to see if a cpage is in buffer
        - returns cpage, True if it is // None, False if it isn't
    '''
    def in_buffer(self, path):
        for cpage in self.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False



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
        # merge_base_pages, merge_tail_pages = self.id_merge_pages()
        #
        # merge_RID_dir = copy.copy(self.buffer_pool.meta_data.key_dir)
        # merge_RID_dict = {}
        # Identify base pages and tail pages we will be using during merge, add them to the appropriate arrays

        # 2. Make a copy of those base_pages
        # 3. Get tail_pages corresponding to the base_pages you want to merge
            # 3.1 You can do this by using the BaseRID column(Which tells you which tail_page corresponding to the base_page)
        # 4. If you have multiple tail_pages, use the most updated one
        # 5. The base_page_copy's values then becomes the tail_pages value(iterate backwards)
        # 1. Get the base_pages you want to merge (Based off of something)

        # self.merge_paths = []
        # self.corresponding_tail_pages = [(BP, tail_page_object)]
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
