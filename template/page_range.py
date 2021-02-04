
class PageRange:

    def __init__(self):
        # 1st index is for base pages
        # 2nd index is for tail pages
        self.range = [[],[]]
        self.num_base_pages = 0
        self.num_tail_pages = 0

    def return_page(self):
        pass

    def full(self):
        return self.num_base_pages == 16

    def append_base_page(self,conceptual_page):
        self.num_base_pages += 1
        self.range[0].append(conceptual_page)

    def append_tail_page(self,conceptual_page):
        # map our base pages to tail pages
        self.range[1].append(conceptual_page)

    def merge(self,pages):
        pass
