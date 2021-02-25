from config import *
from page import Page
import pickle
import os
#we probably want meta data to be stored in the files

class BufferPool():
    def __init__(self):
        self.max_capacity = 16
        self.capacity = 0
        self.array = [None] * self.max_capacity #array of pages
        self.next_evict = 0
    
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

    def evict(self):   #evict a physical page from bufferpool (LRU)
        self.array[self.next_evict] = None  #dont know if this actually deletes the page
        self.next_evict += 1

    
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


