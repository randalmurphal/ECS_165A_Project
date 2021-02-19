from config import *
from Page import Page

class BufferPool():

    def __init__(self):
        # Fixed constant number of records allowed in bufferpool at any given time
        self.max_records   = 0
        # Might be amount of records
        self.cache_records = []
        self.last_accessed = []

    def isFull(self):
        return max_records >= our_num_here


    def dirtyPages(self):
        pass
        # If there are dirty pages in our bufferpool
    def add_page_range(self,page_range):
        if max_records.isFull():
            evict_page_range()
            cache_record = pickle.load("some file path")
        else:
            # Pull from the disk
        pass

    def evict_page_range(self):
        if self.dirtyPages():
            # Write back to the disk
        else:
            # Do nothing
        pass



    def update_dirty_pages(self):
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
