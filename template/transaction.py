from template.table import Table, Record
from template.index import Index
from template.logger import Logger
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries     = []
        self.query_pages = []
        self.logger      = Logger()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, *args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        trans_num = threading.get_ident()
        # Create a new thread here?
        for query, *args in self.queries:
            result = query(*args)
            # Log query success
            query_type = query.__name__
            if query_type == "sum":
                log_msg = "%i, %s, %i, %i"%(trans_num, query_type, args[0], args[1])
            elif query_type == "select":
                log_msg = "%i, %s, %i, %i"%(trans_num, query_type, args[0], args[1])
            else:
                log_msg = "%i, %s, %i"%(trans_num, query_type, args[0])
            self.logger.write_log(log_msg)
            # If the query has failed the transaction should abort
            if result == False:
                # note where we failed
                return self.abort()

        return self.commit()

    def abort(self):
        trans_num  = threading.get_ident()
        trans_logs = self.logger.get_log(trans_num)



        return False

    def commit(self):
        # TODO: commit to database, write to disk
        return True


'''
- Locking:
    - Lock when writing only, minimize time being locked
    - Dictionary in table object and we add a path into the dict when a page is
        locked. Remove from dictionary when unlocked
    - Check if Physical Page is locked when writing (in page.py)
        - if locked, abort
- Pinned:
    - Go thru every thread and add +=1 for the page, -=1 when finished using the page
    - Can't evict if pinned
    - Change isPinned to a counter instead of boolean and when == 0 it is not pinned
- Abort:
    - Call abort if thread is trying to write(update,insert,delete) to page that is locked
    - If aborted, remove from bufferpool without writing back to disk
    - When successful, call commit() to write to disk
'''
