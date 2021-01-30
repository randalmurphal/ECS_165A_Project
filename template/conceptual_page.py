from config import *
from page.py import Page
# 4096 total records
class ConceptualPage:

    def __init__(self):
        self.pages = []
        self.num_records = 0

    def update_num_records(self,page):
        self.num_records += 1

    def full(self):
        if self.num_records > 4096:
            return True
        # Check if Full, so if bytes go over 4096 or 4 KB
        return False

    def add_column(self):
        self.pages.append([])

    def add_page(self, page):
        self.pages.append(Page())
    
    def update_RID(self):
        pass



