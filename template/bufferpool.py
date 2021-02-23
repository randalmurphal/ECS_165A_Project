from config import *
from Page import Page
from table import Table
import pickle
import os

class meta_data():
    # data = some tuple of info we can extract below
    def __init__(self, data):
        # curr_table & curr_page_range is the currently opened file
        self.curr_page_range = 0
        self.curr_base_range = 0
        self.curr_physical_range = 0
        # last_table & last_page_range keep track of the most recently created table and page_range
        self.last_page_range = 0
        self.last_base_range = 0
        self.last_physical_range = 0
        # currently opened curr_baseRID and curr_tailRID
        self.curr_baseRID = 0
        self.curr_tailRID = 0
        # Key:(table,page_range)
        # Might want to make this a file
        self.key_dir = {} #Which file to look at
        # T1PR1B1P1

class BufferPool():

    def __init__(self, meta_data,table_name):

        # Fixed constant number of records allowed in bufferpool at any given time
        # self.table = table
        # Table1 -> Buffer(meta_data,Table1) Table2->Buffer(meta_data,Table2)
        # self.table_name =
        self.table_name = table_name
        self.meta_data     = meta_data(data)
        self.max_records   = 0
        # Might be amount of records
        self.cache_records = [] # The records currently undergoing transactions (the pages containing these records are PINNED)
        self.last_accessed = []
        self.actual_page_ranges = [] #Holds the page_ranges(this mimics a table)
        self.key_dict      = {} #Holds the record values
        self.last_page_ranges = {} # table name or something -> last page range index

    def write_to_disk(self, Record, table):
        # Takes a record object to put information into appropriate column directory
        '''
            - Check to see if file exists
                - if exists then want to write over file with updated data
                - if doesnt exists then just create new file at last index
        '''
            table_name = './ECS165/'+ str(self.table.name)
            page_range_dir = table_name + '/PR' + str(self.meta_data.last_page_range)   # directory for page range
            concept_page_dir = page_range_dir + '/CP' + str(self.meta_data.last_base_range)   # subdirectory for conceptual
            column_directories = []
            for i in LIST_OF_COLUMNS:
                column_page_dir = concept_page_dir + '/Col_Num' + str(i)
            physical_page_name =  concept_page_dir + '/PP' + str(self.meta_data.last_physical_range) +'.pkl' # path for physical page
            path = physical_page_name
            try:
                os.mkdirs(concept_page_dir)
            else:
                print("Something directory already exists")
            with open(path, 'w') as db_file:
                # path = ./ECS165/Grades/PR1/CP1/Col_num1/PP1
                pickle.dump("This thing comes from insert", db_file)


    def update_to_disk(self):
        '''
        Opens up a page, then writes to it
        /blah/conceptualpage/0
        /blah/conceptualpage/1

        '''
        # Compile together N physical pages =(Location_Col0, Location_Col1, Location_Col2)
        # Fetches us 1 physical Page Key: (Path, record num)
        # Key:(path,record num)
        # Get arbitrary key in page_range to see which file it belongs to
        # 1. Get the path, from key
        key =
        key = page_range.range[0].pages[4][0].retrieve(0)
        if key in self.meta_data.key_dir.keys():
            loc  = self.meta_data.key_dir[key] # (table_name, page_range)
            path = './disk/'+loc[0]+'_PR'+str(loc[1])+'.pkl'
        with open(path, 'w') as db_file:
            # path = ./ECS165/Grades/PR1/CP1/PP1
            pickle.dump("This thing comes from insert", db_file)


    def read_from_disk(self, key):
        '''
            Figure out which page range & table
                - then loop through directory for pagerange file
        '''
        myFile = self.meta_data.key_dir[key] #(table, page_range)
        path   = './disk/T'+str(myFile[0])+'_PR'+str(myFile[1])+'.pkl'
        with open(path, 'r') as db_file:
            page_range = pickle.load(db_file)
            self.add_page_range(page_range)

    '''
        reads value from bufferpool if exists,
        else adds record then reads from bufferpool
    '''

    def read_record(self, key, value, column):
        '''
        if page not in bufferpool:
            self.read_from_disk():
        else:
            grab from bufferpool
        '''
        if key not in self.key_dict.keys(): #if page range is not in our bufferpool
            # swap/add page range we need into bufferpool -> get_record(loc)
            self.add_page_range(key)

        loc = self.key_dict[key] # (table, page_range, concept_page, page)
        return self.get_record(loc)

        # Search through all keys in key_dict for the page_range the record is on

    '''
        Search through page ranges in bufferpool to actually get that record values
    '''
    def get_record(self, loc):
        page_range = loc[1]
        # get values at page_range
        values = self.page_directories[page_range]...
        return values
        pass

    def isFull(self):
        return max_records >= our_num_here

    def dirtyPages(self):
        pass
        # If there are dirty pages in our bufferpool
    def add_page_range(self, key):
        if max_records.isFull():
            evict_page_range()
            cache_record = pickle.load("some file path")
        else:
            # Pull from the disk
        pass

    def evict_page_range(self):
        if self.dirtyPages():
            # Write back to the disk
            pass
        else:
            # Do nothing
            pass



    def update_dirty_pages(self):
        pass






    def mess(self):
        pass



#bufferpool class
    #starts w/ nothing
    #array of tail and base pages
    #counter in both, to keep the num of pages
    #store a whole table in bufferpool

    # Functions:
    # add/fetch page (pin page)
    # evict pages (unpin page) (if dirty)
    # re-store updated pages
    # update/write to page
