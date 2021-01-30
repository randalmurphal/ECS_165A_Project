from template.config import *


class Page:

    def init(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records >=512 :
            return False
        # Check if Full, ensures there are less than 512 records stored in the page
        return True

    def write(self, value):
        # Write into the page the value
        #1) Convert to bytes
        '''
        def convert_val_to_bytes(value):
            
            return val_to_bytes # return a list of 8 bytes made from value
            
        val_to_bytes = convert_val_to_bytes(value)
        '''
        #2) Identify the index of the next available spot in the bytearray
        start_index = self.num_records*8 - 1
        
        #3) Insert each byte into appropriate index in bytearray
        for byte_num in range(8):
            self.data[start_index+byte_num] = val_to_bytes[byte_num]
        
        #4) Update the number of records the page contains
        self.num_records += 1
        return True
