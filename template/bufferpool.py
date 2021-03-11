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
        # self.key_dict_locked = False # When merging lock to prevent insert changes

class BufferPool():

    def __init__(self, table_name, num_columns):
        self.meta_data        = MetaData() # holds table meta data
        self.max_capacity     = 64         # max number of cpages in buffer
        self.conceptual_pages = []         # cpage objects
        self.buffer_keys      = {}         # page_path: page_object
        self.table_name       = table_name # name of table
        self.merge_bases      = []         # holds bp paths ready to merge
        self.merge_tails      = []         # holds tail pages for merge bases
        self.first            = True       # for manual check to make first PR
        self.trans_recs       = {}         # holds [(location, path, "insert/update")] changed in each transaction


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
            success = self.populateConceptualPage(columns, cpage)
            if not success:
                return None
        self.addConceptualPage(cpage)
        return cpage

    '''
        Adds a cpage to buffer_pool while following eviction policy
    '''
    def addConceptualPage(self, conceptualPage):
        # evict until space in buffer
        if len(self.conceptual_pages) >= self.max_capacity:
            print("\n\nlen cpage: %i\n\n"%len(self.conceptual_pages))
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
            success = conceptualPage.pages[i + offset].write(columns[i])
            if not success:
                return False
        conceptualPage.pages[3].append(np.zeros(len(columns)))
        return True

    '''
        Evict called when buffer_pool needs to remove a cpage from memory
        - Loops through to find first page not pinnex (waits until finds one)
        - Writes to disk if page is dirty, else just remove from buffer
    '''
    def evict(self):   #evict a physical page from bufferpool (LRU)
        # print("\n\n --- EVICT --- \n\n")
        i = 0
        cpage = self.conceptual_pages[i]
        # Loop through until finds an unpinned page
        while cpage.isPinned > 0:
            i += 1
            if i == len(self.conceptual_pages):
                i = 0
            cpage = self.conceptual_pages[i]
        cpage = self.conceptual_pages.pop(i) # remove and store unpinned cpage
        cpage.isPinned -= 1
        self.remove_key(cpage)
        # if changes to write, page is dirty
        if cpage.dirty:
            path = cpage.path
            cpage.dirty = False
            with open(path, 'wb') as db_file:
                pickle.dump(cpage, db_file)

    # '''
    #     Removes a page from bufferpool when aborting
    # '''
    # def remove(self):


    '''
        Close: evicts all cpages from buffer_pool
    '''
    def close(self):
        # print("\n\n---CLOSING---\n\n")
        while self.conceptual_pages:
            self.conceptual_pages[0].isPinned = 0
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
        for i, path in enumerate(self.merge_bases):
            base_page, is_in_buffer = self.in_buffer(path)
            if not is_in_buffer:
                with open(path, "rb") as db_file:
                    base_page = pickle.load(db_file)
            base_pages.append(base_page)

        return base_pages

    def get_tail_pages(self, base_page):
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
        # baseRID:(tail_path,tail_RID)
        # go through each
        for base_rec_num in range(new_base_page.num_records):
            base_rec_RID = new_base_page.pages[1].retrieve(base_rec_num)
            # if not in indirection, we don't need to check for updates because it hasn't been updated
            if not base_rec_RID in indirection.keys():
                continue
            tail_page_path, tail_rec_ind, _ = indirection[base_rec_RID]
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
        # Abstract out the numbers from the file_string
        path = base_page.path
        pr_num = path.split('/')[4][2:]
        bp_str = path.split('/')[5].split('_')
        bp_num = bp_str[0][2:]
        m_num  = base_page.merge_num

        for rec_ind in range(base_page.num_records):
            record_key = base_page.pages[6].retrieve(rec_ind)
            new_loc = (pr_num, bp_num, m_num, rec_ind)
            self.meta_data.key_dict[record_key] = new_loc

    '''
        1. Get a base_page from the set of possible merges
        2. Get the corresponding tail_pages
        3. Create a new copy of the base_page
        4. Merge the tail_pages with the new base_page
        5. Create a new file called # Path = ./ECS165/Grades/PR#/BP#_M#
        6. Go through the key_dict and find all the Keys that are equalivent
            to the keys in the new_base_page and replace path
    '''
    def merge(self):
        # print("begin merge")
        base_pages = self.get_base_pages()
        i = 0
        for base_page in base_pages:
            if base_page.isPinned:
                i += 1
                continue
            merge_num = base_page.merge_num
            tail_pages = self.get_tail_pages(base_page)
            new_base = self.create_base_copy(base_page, tail_pages)
            new_base_path = self.set_new_path(new_base, merge_num)
            self.change_dict_vals(new_base, new_base_path)
            self.merge_bases.pop(i)
            base_page.merge_num += 1
        # print("end merge")
        return

    '''
        Checks to see if a cpage is in buffer
        - returns cpage, True if it is // None, False if it isn't
    '''
    def in_buffer(self, path):
        for cpage in self.conceptual_pages:
            if cpage.path == path:
                return cpage, True
        return None, False
