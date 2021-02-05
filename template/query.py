from table import Table, Record
from index import Index
from conceptual_page import ConceptualPage
from page_range import PageRange
from page import Page

import numpy as np
import time
import math

MAX_INT = int(math.pow(2, 63) - 1)

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass
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

    def add_meta(self, new_base, page_index, values):
        new_base.pages[1][page_index].write(values[0])
        new_base.pages[2][page_index].write(values[1])
        new_base.pages[3].append(np.zeros(len(new_base.pages) - 4))

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # Tuple of columns(Key,Value)
    def insert(self, *columns):
        # bc sum function asks for 10,001 key, but only have 10k
        if not self.table.init_key:
            self.table.init_key = columns[0]
        new_page_range   = self.table.RID_count % 65536 == 0
        page_range_index = self.table.RID_count // 65536
        new_base_page    = self.table.RID_count % 4096 == 0
        base_page_index  = (self.table.RID_count % 65536) // 4096
        new_page         = self.table.RID_count % 512 == 0
        page_index       = (self.table.RID_count % 4096) // 512
        record_index     = self.table.RID_count % 512

        new_base  = ConceptualPage(columns)
        new_range = PageRange()

        if new_page_range:
		    # create new page range
            new_range.append_base_page(new_base) # [0].append(new_base)
            self.table.page_directory.append(new_range)
            for i, col in enumerate(columns):
                new_base.pages[i+4][page_index].write(col)

            values = [self.table.RID_count, 0, 0]
            self.add_meta(new_base, page_index, values)
        else:
            if new_base_page:
                self.table.page_directory[page_range_index].append_base_page(new_base)

                values = [self.table.RID_count, 0, 0]
                self.add_meta(new_base, page_index, values)

                for i, col in enumerate(columns):
                	new_base.pages[i+4][page_index].write(col)
            else:
                if new_page:
		            # append new page to current conceptualpage
                    new_base = self.table.page_directory[page_range_index].range[0][base_page_index]
                    for i in range(len(new_base.pages)):
                        if not i == 0 and not i == 3:
                            new_base.pages[i].append(Page())

                    for i, col in enumerate(columns):
                        new_base.pages[i+4][page_index].write(col)

                    values = [self.table.RID_count, 0, 0]
                    self.add_meta(new_base, page_index, values)
                else:
                    new_base = self.table.page_directory[page_range_index].range[0][base_page_index]

                    for i, col in enumerate(columns):
                        new_base.pages[i+4][page_index].write(col)

                    values = [self.table.RID_count, 0, 0]
                    self.add_meta(new_base, page_index, values)

        key      = columns[0]
        location = (page_range_index, base_page_index, page_index, record_index)
        self.table.key_dict[key] = location
        self.table.RID_count += 1
        new_base.num_records += 1
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
        location = self.table.key_dict[key] # Assume all keys have been inserted

        p_range, base_pg, page, record = location
        base_pages = self.table.page_directory[p_range].range[0][base_pg].pages
        rid     = base_pages[1][page].retrieve(record)
        key     = base_pages[4][page].retrieve(record)
        rec_i   = page * 512 + record
        base_schema = base_pages[3][rec_i]
        indirection = base_pages[0]

        all_columns = []
        # populate with base page values
        for i in range(len(query_columns)):
            all_columns.append(base_pages[i+4][page].retrieve(record))

        # Grab updated values in tail page
        if rid in indirection.keys():
            tail_rid     = indirection[rid]
            tail_page_i  = (tail_rid % 65536) // 4096
            page_i       = (tail_rid % 4096) // 512
            tail_page    = self.table.page_directory[p_range].range[1][tail_page_i].pages
            # for i, col in enumerate(tail_page[4:]):
            for i, col in enumerate(tail_page[4:]):
                value = col[page_i].retrieve(tail_rid % 512)
                if value != MAX_INT:
                    all_columns[i] = col[page_i].retrieve(tail_rid % 512)

        columns = []
        for i, col in enumerate(query_columns):
            if col:
                columns.append(all_columns[i])

        rec = Record(rid, key, columns)
        return [rec]

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # Figure out which columns we are updating
        query_columns = []
        for i, col in enumerate(columns):
            if col != None:
                query_columns.append(1)
            else:
                query_columns.append(0)

        location = self.table.key_dict[key] # Assume all keys have been inserted
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
