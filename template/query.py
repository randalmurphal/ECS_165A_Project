from table import Table, Record
from index import Index
from conceptual_page import ConceptualPage
from page_range import PageRange
from page import Page
from bufferpool import BufferPool

from datetime import datetime
import numpy as np
import math

MAX_INT = int(math.pow(2, 63) - 1)
MAX_PAGE_RANGE_SIZE = 4096
MAX_BASE_PAGE_SIZE  = 512
MAX_PHYS_PAGE_SIZE  = 512

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, bufferpool):
        self.bufferpool = bufferpool
        data = 'rip' #idk

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    # You can tell if a Base Record has been "Deleted" by checking the Indirection
    column and the Schema encoding column.
      If the Base record is "Deleted" it will have all zeros in its Schema Encoding
      column AND STILL point towards a tail record through its indirection column.
    """
    def delete(self, key):
        # Grab location of base record
        baseR_loc = self.table.key_dict[key]
        baseR_p_range, baseR_base_pg, baseR_pg, baseR_rec = baseR_loc
        base_pages    = self.table.page_directory[baseR_p_range].range[0][baseR_base_pg].pages
        base_rid      = base_pages[1][baseR_pg].retrieve(baseR_rec)
        base_schema_i = MAX_PHYS_PAGE_SIZE*baseR_pg + baseR_rec
        # Check indirection column to see if has been updated
        indirection = base_pages[0]
        updated     = base_rid in indirection.keys()
        n_cols      = self.table.num_columns

        # If not updated, add tail page with MAX_INT vals and add to indirection
        if not updated:
            # Update to add tail page with None for all values
            self.update(key, *[None]*n_cols)
        else:
            # Change base schema to all 0's, then update which gives None tail page
            base_pages[3][base_schema_i] = np.zeros(n_cols)
            self.update(key, *[None]*n_cols)

        return True



    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
		'''
		1. Create a new empty page and add to disk
		2. Read that page from disk into bufferpool
		'''
        # MAKE RECORD OBJECT FOR EASY HANDLING
        key = *columns[0]
        current_RID = self.bufferpool.meta_data.curr_baseRID

        # Add Record Meta Data to Record Object
        new_record = Record(current_RID, key, *columns)
        new_record.indirection = MAX_INT
        new_record.time = self.current_time()
        new_record.schemaEncoding = np.zeros(len(*columns)) 
        new_record.TPS = MAX_INT
        new_record.baseRID = MAX_INT

        # Find current page to insert record into
        current_base_page = self.bufferpool.find_conceptual_page_for_query(None, "Insert")

        
        if current_base_page.full(): # if latest base page we were inserting into is full
            # Create a new base page
            new_base_page = ConceptualPage(*columns)

            # Mark new base page as pinned because we are performing a transaction on it
            new_base_page.isPinned = True

            # add created base page to bufferpool
            self.bufferpool.add_conceptual_page(new_base_page)

            # Write record into the new base page, mark it as dirty
            new_base_page.insert_record(new_record)
            new_base_page.isDirty = True

            # add record key into key index of bufferpool and key_dir of BF metadata
            self.bufferpool.key_dict[new_record.key] = new_base_page
            self.bufferpool.meta_data.insertion_conceptual_page = new_base_page
            
            # unpin both pages
            current_base_page.isPinned = False
            new_base_page.isPinned = False

            

        else: # latest base page is not full
            current_base_page.insert_record(new_record)
            current_base_page.isDirty = True
            self.bufferpool.key_dict[new_record.key] = current_base_page

            current_base_page.isPinned = False
            
        self.table.RID_count += 1


        return True
            

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
   def select(self, key, column, query_columns):
		'''
		1. Look at bufferpool, if exist -> perform select; else -> pull into bufferpool
		2.
		'''
        # Purpose of select: Return a record with most updated values
            # 1. Get BasePage location
            base_and_tail_page=  self.bufferpool.find_conceptual_page_for_query(key,'Select')
            base_page = base_and_tail_page[0]
            # 2. Get the BasePage values into all_columns
            all_columns = []
            for i in range(len(query_columns)):
                all_columns.append(base_page[i+4][physical_page_loc].retrieve(record))
            tail_page_location = self.bufferpool.find_conceptual_page_for_query(key,'Select')
            # 3. Get the most updated values of TailPages
                # 3.1 If TailPage has None values, get Base_page record
            
        location = self.table.key_dict[key] # Assume all keys have been inserted

        p_range, base_pg, page, record = location
        # Returns physical pages corresponding to base_page(AKA columns)
        base_pages = self.table.page_directory[p_range].range[0][base_pg].pages
        # Get the RID column, then gets the value of the baseRID
        rid     = base_pages[1][page].retrieve(record)
        # Gets the indirection column of base_page
        indirection = base_pages[0]

        all_columns = []
        # populate with columns w/ base page values
        for i in range(len(query_columns)):
            all_columns.append(base_pages[i+4][page].retrieve(record))

        # Grab updated values in tail page
        if rid in indirection.keys():
            # baseRID:tailRID
            # 
            tail_rid     = indirection[rid]
            tail_page_i  = (tail_rid % MAX_PAGE_RANGE_SIZE) // MAX_BASE_PAGE_SIZE
            page_i       = (tail_rid % MAX_BASE_PAGE_SIZE) // MAX_PHYS_PAGE_SIZE
            tail_page    = self.table.page_directory[p_range].range[1][tail_page_i].pages
            for i, col in enumerate(tail_page[4:]):
                value = col[page_i].retrieve(tail_rid % MAX_PHYS_PAGE_SIZE)
                if value != MAX_INT:
                    all_columns[i] = col[page_i].retrieve(tail_rid % MAX_PHYS_PAGE_SIZE)

        columns = []
        for i, col in enumerate(query_columns):
            if col:
                columns.append(all_columns[i])

        key = base_pages[4][page].retrieve(record)
        rec = Record(rid, key, columns)
        return [rec]

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        createSnapShot = False
        def createSnapShot():
            # Get Original value from schema encoding changed and then create a tail record for it

        def createTailPage():
            #1. Figure out which columns we are updating
            query_colums = []
            for i, col in enumerate(columns):
                if col != None:
                    query_columns.append(1)
                else:
                    query_columns.append(0)
            # Figure out which columns we are updating
            record = self.bufferpool.read_record()
            #2. Get location of the base_page
            # Note: We need to see if the key is in conceptual_pages first
            conceptualPages = self.bufferpool.conceptual_pages
            # Look through the conceptual_pages to see if the key is there
                # If it get that record using key_dict?
                # Else fetch that record from the disk in BufferPool
            location = self.bufferpool.conceptual_pages[key]
                # 2.1 Change Schema Encoding
                # 2.2 Change Indirection Column


        location = self.table.key_dict[key] # Assume all keys have been inserted
        p_range_loc, b_page_loc, page_loc, record_loc = location

        base_page  = self.table.page_directory[p_range_loc].range[0][b_page_loc].pages

        indirection = base_page[0]
        page_ind    = MAX_PHYS_PAGE_SIZE*page_loc + record_loc
        base_schema = base_page[3][page_ind]
        record_rid  = base_page[1][page_loc].retrieve(record_loc)
        cols        = []

        tail_RID = self.table.page_directory[p_range_loc].tail_RID
        prev_tail_RID = tail_RID
        # Base Page stuff
        if record_rid in indirection.keys(): # if update has happened already (ie tail page exists for record)
            tail = indirection[record_rid]
            tail_page_i  = tail // MAX_BASE_PAGE_SIZE
            page_i       = (tail % MAX_BASE_PAGE_SIZE) // MAX_PHYS_PAGE_SIZE
            record_i     = tail % MAX_PHYS_PAGE_SIZE
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
        self.table.page_directory[p_range_loc].tail_RID += 1

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
        if tail_pages[-1].num_records % MAX_PHYS_PAGE_SIZE == 0:
            for i, col in enumerate(tail_pages[-1].pages):
                # Not indirection & schema
                if not i == 0 and not i == 3:
                    col.append(Page())
        tail_page_i = tail_pages[-1].num_records // MAX_PHYS_PAGE_SIZE
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
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
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



    def current_time():
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
        return time_val
