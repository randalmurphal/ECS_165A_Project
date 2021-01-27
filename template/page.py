from template.config import *


class Page:

    def init(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if self.num_records >= 4096:
            return False
        # Check if Full, so if bytes go over 4096 or 4 KB
        return True

    def write(self, value):
        # Write into the page the value
        self.data[self.num_records] = value
        #1) Convert to bytes
        # Put the value into the next available space into self.data(this is an array of bytes)
        self.num_records += 1
        return True