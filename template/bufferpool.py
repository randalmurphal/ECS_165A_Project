from config import *
from Page import Page
from table import Table
import pickle
import os

class meta_data():
    # data = some tuple of info we can extract below
    def __init__(self, data):
        # curr_table & curr_page_range is the currently opened file
        self.curr_page_range = 1
        self.curr_base_range = 1
        self.curr_physical_page = 0
        # last_table & last_page_range keep track of the most recently created table and page_range
        self.last_page_range = 1
        self.last_base_page = 1
        self.last_physical_page = 0 # THIS WILL BE THE PATH TO THE 
        # currently opened curr_baseRID and curr_tailRID
        self.curr_baseRID = 0
        self.curr_tailRID = 0
        # Key:(table,page_range)
        # Might want to make this a file
        self.key_dir = {} #Which file to look at

        self.newPageNecessary = False

        # For Inserting Records
        self.insertion_conceptual_page = 
        self.insertion_conceptual_page_path = 
        

class BufferPool():

    def __init__(self, meta_data,table_name):

        # Fixed constant number of conceptual pages allowed in bufferpool at any given time
        # self.table = table
        # Table1 -> Buffer(meta_data,Table1) Table2->Buffer(meta_data,Table2)
        # self.table_name =

        self.table_name = table_name
        self.meta_data     = meta_data(data)
        # self.max_records   = 0
        # Might be amount of records
        # self.cache_records = [] # The records currently undergoing transactions (the pages containing these records are PINNED)
        # self.last_accessed = [] 
        self.conceptual_pages = []    #Holds the conceptual_Pages

        self.key_dict      = {} #Holds the record values
        self.last_page_ranges = {} # table name or something -> last page range index


    def write_to_disk(self,table,key_value,record_values):
        '''
        table = table_name given to us
        key_value = the key given to us from insert
        record_values = the columns in a conceptual page in array form
        '''
        # Writes a base page into disk, basically only on initalization of base page

        '''
            - Check to see if file exists
                - if exists then want to write over file with updated data
                - if doesnt exists then just create new file at last index
        '''

            # Get the key_value from insert
            table_name = './ECS165/'+ str(self.table.name)
            page_range_dir = table_name + '/PR/' + str(self.meta_data.last_page_range)   # directory for page range
            concept_page_dir = page_range_dir + '/CP/' + str(self.meta_data.last_base_page)   # subdirectory for conceptual
            self.meta_data.key_dir[key_value] = (self.table.name,self.meta_data.last_page_range,self.meta_data.last_base_page)
            # column_directories = []
            try:
                os.mkdirs(concept_page_dir)
            else:
                print("Something directory already exists")
            for i in range(len(record_values)):
                # Write into physical pages
                 columns = concept_page_dir + '/PP/' + str(i) + '.pkl'
                 # path = ./ECS165/Grades/PR1/CP1/PP1
                 path = columns
                 with open(path,'w') as db_file:
                    #  Will insert record_value into it's respective physical page
                     pickle.dump(record_values[i],db_file)
            



    def update_to_disk(self,update_data,evicted_page):
        '''
        Opens up a conceptual page, then writes values into the physical pages(these values come when conceptual pages are evicted)
        ./ECS165/Grades/PR1/CP1/PP1
        update_data = The data we want to add into the disk
        evicted_page = The page that we evicted 
        '''
        # Compile together N physical pages =(Location_Col0, Location_Col1, Location_Col2)
        # Fetches us 1 physical Page Key: (Path, record num)
        # Key:(path,record num)
        # Get arbitrary key in page_range to see which file it belongs to
        # 1. Get the path, from key

        # 1. Get the key value of the conceptual_page being evicted or merge
            # Note pages[4], the 4 is the col of key
        key_of_page = evicted_page.pages[4][0].retrieve(0)
        # 2. From here we can use the key_dir to get the location

        if key_of_page in self.meta_data.key_dir.keys():
            loc  = self.meta_data.key_dir[key_of_page] # (table_name, page_range,concept_page)
            path = './ECS165/'+loc[0]+'/PR/'+str(loc[1])+ '/CP/' + str(loc[2])
            # Go through the cols to write into the physical pages
            for i in range(len(num_cols)):
                # Goes into the appropriate physical page
                columns = path + '/PP/' + str(i)
                 # path = ./ECS165/Grades/PR1/CP1/PP1
                with open(path, 'w') as db_file:
                    # evicted_page.pages[i] will give you the proper page in ram
                    pickle.dump(evicted_page.pages[i], db_file)


    def read_from_disk(self, key):
        # we can assume that we have already confirmed the record we need is not in the bufferpool
        '''
            Figure out which page range & table
                - then loop through directory for pagerange file
            1. Check and see if current base page we are inserting records into (insertion base page) is in bufferpool. If insertion base page is in bufferpool and it is not pinned: proceed with insertion. 
            IF INSERTION BASE PAGE IS PINNED DONT PROCEED WITH INSERTION
            2. If insertion base page is not in bufferpool => check if bufferpool is full
            3. If bufferpool is not full, pull insertion base page from disk into bufferpool
            4. If bufferpoll is full, evict LRU base page then pull current base page into bufferpool
            SIDE: Any time an insertion is completed and it fills up the current insertion base page: create new insertion base page
        '''

        # 1.Go to directory and then pull physical pages to create a conceptual page
        myFile = self.meta_data.key_dir[key] #(table_name, page_range,concept_page)
        # path = ./ECS165/Grades/PR1/CP1
        path = './ECS165/' + myFile[0] + '/PR/' + str(myFile[1]) + '/CP/' + str(myFile[2])
        
        with open(path,'r') as db_file:
            conceptual_page = pickle.load(db_file)
            return conceptual_page
        
            

    '''
        reads value from bufferpool if exists,
        else adds record then reads from bufferpool
    '''
    def find_conceptual_page_for_query(self, key, query_type):
        '''

        

        '''

        if query_type == "Insert"

            insertion_base_page = self.meta_data.insertion_conceptual_page

            insertion_base_page.isPinned = True  # marks base page as pinned because it is currently undergoing a transaction
            

            if insertion_base_page in self.conceptual_pages:          # if the base page we are inserting into is currently in bufferpool
                self.conceptual_pages.remove(insertion_base_page)     # puts accessed base page as most recently used
                self.conceptual_pages.insert(0, insertion_base_page)  # puts accessed base page as most recently used
                return insertion_base_page
            
            else:
                insertion_base_page = self.meta_data.insertion_conceptual_page = self.read_from_disk(self.insertion_conceptual_page_path) 
                insertion_base
                self.add_conceptual_page(self.meta_data.insertion_conceptual_page)

                return self.meta_data.insertion_conceptual_page
        
        elif query_type == "Update":
            # FINISH PART FOR QUERY UPDATE
            if key in self.key_dict.keys():

        elif query_type == "Select":
            # FINISH PART FOR QUERY SELECT


        elif query_type == "Sum":
            # FINISH PART FOR QUERY SUM 


                

        base_page_index_in_bufferpool = 

        return base_page_index_in_bufferpool



    def read_record(self, key, value, column):
        '''
        if page not in bufferpool:
            self.read_from_disk():
        else:
            grab from bufferpool
        '''
        if key in self.key_dict.keys(): #if page range is not in our bufferpool
            # swap/add page range we need into bufferpool -> get_record(loc)
            self.add_page_range(key)

        loc = self.key_dict[key] # (table, page_range, concept_page, page)
        return self.get_record(loc)

        # Search through all keys in key_dict for the page_range the record is on
    
    def add_conceptual_page(self, conceptual_page):

        '''
        1. Check if the Bufferpool is full. If bufferpool is full, evict LRU Conceptual Page.
        2. Insert new conceptual page at index 0
        '''

        if self.isFull():
            self.evict_conceptual_range()
            self.conceptual_pages.insert(0, conceptual_page)
            self.add_record_keys_to_key_dict(conceptual_page)

        else:
            self.conceptual_pages.insert(0, conceptual_page)
            self.add_record_keys_to_key_dict(conceptual_page)


    def evict_conceptual_page(self):
        '''
        Finds least recently used conceptual page in bufferpool that is NOT PINNED.
        Determine if said conceptual page is dirty.
        If conceptual page is dirty --> write it back to disk then remove it from bufferpool
        If conceptual page is NOT dirty --> remove 
        '''
        
        conceptual_page_to_evict = None
        while conceptual_page_to_evict == None:
            for i in range(15,0):
                if self.conceptual_pages[i].isPinned:
                    continue
                else:
                    conceptual_page_to_evict = conceptual_pages[i]
                    break
        
        if conceptual_page_to_evict.isDirty:
            self.write_to_disk(conceptual_page_to_evict, self.table_name)
            self.conceptual_pages.remove(conceptual_page_to_evict)
            self.remove_record_keys_from_key_dict(conceptual_page_to_evict)

        else:
            self.conceptual_pages.remove(conceptual_page_to_evict)
            self.remove_record_keys_from_key_dict(conceptual_page_to_evict)





    '''
    Search through page ranges in bufferpool to actually get that record values
    '''
    def get_record(self, loc):
        page_range = loc[1]
        # get values at page_range
        values = self.page_directories[page_range]...
        return values
        pass



    def remove_record_keys_from_key_dict(self, conceptual_page):
        for i in range(conceptual_page.num_records):
            key_i = conceptual_page.pages[6].retrieve(i)
            del self.key_dict[key_i]

    def add_record_keys_to_key_dict(self, conceptual_page):
        for i in range(conceptual_page.num_records):
            key_i = conceptual_page.pages[6].retrieve(i)
            self.key_dict[key_i] = conceptual_page

    def isFull(self):
        return len(self.conceptual_pages) >= 16




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
