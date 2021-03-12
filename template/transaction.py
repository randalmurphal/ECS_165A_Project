from template.table import Table, Record
from template.index import Index
from template.logger import Logger
import threading, copy, math
import numpy as np
MAX_INT = int(math.pow(2, 63) - 1)

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries     = []
        self.query_pages = []
        self.logger      = Logger()
        self.query_obj   = None
        self.completed_t = {}
        self.first_update = True
        self.first_select = True
        self.first_insert = True

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
    def run(self, query_obj):
        self.query_obj = query_obj
        trans_num = threading.get_ident()
        self.query_obj.trans_num = trans_num
        for query, *args in self.queries:
            query_type = query.__name__
            if query_type == "sum":
                # Logs start & end ranges
                log_msg = "%i, %s, %i, %i\n"%(trans_num, query_type, args[0], args[1])
            elif query_type == "select":
                log_msg = "%i, %s, %i, %i\n"%(trans_num, query_type, args[0], args[1])
            else:
                log_msg = "%i, %s, %i\n"%(trans_num, query_type, args[0])
            self.logger.write_log(log_msg)
            result = query(*args)

            # Log query success
            # If the query has failed the transaction should abort
            if result == False:
                # note where we failed
                print("---ABORT---")
                print("aborted", query_type," : ", trans_num, *args)
                # self.logger.write_log("%i, %s, %i, %s\n"%(trans_num, query_type, args[0], "aborted"))
                return self.abort(trans_num)
        log_msg = "%i, Transaction Complete\n"%trans_num
        self.logger.write_log(log_msg)
        ret = self.commit()
        # print("\n\n---Done Committed---\n\n")
        return ret


    '''
        Before an abort, check bufferpool and revert changes before sending back
        to the transaction abort (which checks through the disk for aborted changes)
    '''
    def abort(self, trans_num):
        logs = self.get_logs(trans_num)
        # append operations and then call function to perform undo
        abort_insert_logs = []
        abort_update_logs = []
        for line in reversed(logs):
            query_type = line.split(',')[1]
            # 2 diff operations for insert and select
            if query_type == " insert":
                abort_insert_logs.append(line)
            else:
                abort_update_logs.append(line)
        # print(abort_insert_logs)
        # print(abort_update_logs)
        # print(self.query_obj.buffer_pool.meta_data.key_dict)
        # print(abort_update_logs)
        self.abort_updates(abort_update_logs)
        self.abort_inserts(abort_insert_logs)
        return False

    '''
    Parses through logs and returns lines of select or update for
    this aborted transaction thread
    returns: array of relevant lines in logs
    '''
    def get_logs(self, trans_num):
        logs = []
        # trans_num = threading.get_ident()
        with open(self.query_obj.logger.log_path, 'r') as log_file:
            lines = log_file.readlines()
            for line in lines:
                split = line.split(',')
                file_trans_num = split[0]
                query_type     = split[1]
                if int(file_trans_num) == trans_num and (query_type == ' insert' or query_type == ' update'):
                    logs.append(line)
        return logs
    '''
        Undoes inserts for transactions still in bufferpool:
            - performs a delete on that record (schema=0's & has
                some tail page in indirection)
    '''
    def abort_inserts(self, insert_logs):
        for log in insert_logs:
            print(log)
            key   = int(log.split(',')[2])
            # if key not in self.query_obj.buffer_pool.meta_data.key_dict.keys():
            #     continue
            loc   = self.query_obj.buffer_pool.meta_data.key_dict[key]
            path  = self.get_log_path(loc)
            cpage = self.query_obj.get_from_disk(path=path)
            self.query_obj.delete(key)
        return

    '''
        Undoes updates for transactions still in bufferpool:
        - Create new tail record
        - Go through each tail record in indirection paths
            - if tail record is from this transaction, dont add that value to new tail record
            - else, add the values that were updated in this tail record to new tail record
        - continue until either all columns of new tail record are filled, or indirection points to itself (end of the chain)
        - point base record indirection to new tail record and new tail record to prev tail record

    ************

        TODO:
        - When updating, update the schema for tail pages
        - Add thread id to tail record in some way
        - Need to make sure that it can abort if that page is being accessed by another thread (when retrieving record rid)
    '''
    def abort_updates(self, update_logs):
        for log in update_logs:
            print("aborting: ", log)
            key  = int(log.split(',')[2])
            if key not in self.query_obj.buffer_pool.meta_data.key_dict.keys():
                continue
            loc  = self.query_obj.buffer_pool.meta_data.key_dict[key] # gets location tuple for record
            path = self.get_log_path(loc)
            rec_ind = loc[3]
            cpage = self.query_obj.get_from_disk(path=path)
            cpage.isPinned += 1
            self.add_abort_tail(loc, cpage)
            cpage.isPinned -= 1

        return
    '''
        Takes a location for query record
        Returns the path of the page for the query
    '''
    def get_log_path(self, location):
        pr_num  = location[0]
        bp_num  = location[1]
        ''' idk what to do about merged files, will think about it later '''
        return './template/%s/'%(self.query_obj.table.path) +self.query_obj.table.name+'/PR'+str(pr_num)+'/BP'+str(bp_num)

    '''
        Gets a new tail page and adds to base indirection
            - new tail rec indirection points to tail page in base rec indirection
                (should be most prev update that is not in this transaction)
    '''
    def add_abort_tail(self, loc, cpage):
        new_vals   = [MAX_INT]*self.query_obj.table.num_columns
        new_schema = np.zeros(self.query_obj.table.num_columns)
        base_indir = cpage.pages[0]
        curr_indir = cpage.pages[0]
        rec_ind    = loc[-1]
        rec_RID    = cpage.pages[1].retrieve(rec_ind) # Check access (cant abort an abort)
        prev_RID   = rec_RID
        tail_RID   = -1
        tail_page  = None
        in_trans   = False
        tail_path, tail_rec_ind, tail_thread_num = curr_indir[prev_RID]
        # Loop until it points to itself, so end of chain
        while True:
            base_indir_val = base_indir[rec_RID]
            # Get tail page in indirection
            # Dont save tail when skipping over update in this transaction
            if not in_trans:
                prev_tail_page    = tail_page
                prev_tail_rec_ind = tail_rec_ind

            tail_path, tail_rec_ind, tail_thread_num = curr_indir[prev_RID]
            # buffer_lock.acquire()
            tail_page, is_in_buffer = self.query_obj.in_buffer(tail_path)
            if not is_in_buffer:
                tail_page = self.query_obj.get_from_disk(path=tail_path)
            tail_page.isPinned += 1
            # buffer_lock.release()
            # Check to see if last tail rec was in transaction, if not then update previous indirection, else dont
            if not in_trans:
                prev_indir = curr_indir
            curr_indir = tail_page.pages[0]
            # If we reached the end of the updates for this record, loops onto itself -- here if last update is not in this transaction
            # Workaround bc dont want to store prev rid in first loop
            tail_RID = tail_page.pages[1].retrieve(tail_rec_ind)
            if tail_RID != 0:
                tail_RID -= 1
            # print(curr_indir)
            # print(prev_indir)
            # if curr_indir[tail_RID] == prev_indir[prev_RID]:
            #     break
            if tail_RID != -1:
                prev_RID = tail_RID
            in_trans = (tail_thread_num == threading.get_ident())
            # if in this transaction, dont take updates & change pointers
            if in_trans:
                # if reached end of updates for this record & want to remove last update
                temp_path, temp_rec_ind, temp_thread_num = curr_indir[tail_RID]
                # if the same page as the indirection is pointing to
                if temp_path == tail_page.path and temp_rec_ind == tail_rec_ind:
                    # If prev_tail_page is None, then we know we are removing all updates.
                    #   Set base page schema to 0's and remove indirection
                    if prev_tail_page == None:
                        del base_indir[rec_RID]
                        base_schema = cpage.pages[3][rec_ind]
                        base_schema = np.zeros(self.query_obj.table.num_columns)
                    else:
                        # set prev indirection to point to itself
                        prev_indir = (prev_tail_page.path, prev_tail_rec_ind)
                    break # -- here if last update is in this transaction and needed to be removed
                # skip over the current tail record in the path of updates (prev -> next vs prev -> curr -> next)
                prev_indir[prev_RID] = curr_indir[tail_RID]
            else:
                values = self.get_updated_tail_cols(tail_page, tail_rec_ind)
                # Only add the most updated values, so if already in new_vals at ind, dont set to older update
                for ind, val in values:
                    if new_vals[ind] == MAX_INT:
                        if val == MAX_INT:
                            new_vals[ind] = MAX_INT
                        else:
                            new_vals[ind] = val
                            # to update base schema after aborted updates
                            new_schema[ind] = 1
            tail_page.isPinned -= 1
        # If we can't just add to current tail page, create new tail page
        if tail_page.full():
            tail_page.isPinned -= 1
            pr_num    = loc[0]
            # buffer_lock.acquire()
            tail_page = self.query_obj.new_tail_page(pr_num) # adds to buffer
            tail_page.isPinned += 1
            # buffer_lock.release()
            tail_rec_ind = 0
        else:
            tail_rec_ind = tail_page.num_records
        # Write new values to page
        self.query_obj.buffer_pool.populateConceptualPage(new_vals, tail_page)
        # Set new tail indirection to most recent update before this
        tail_indir = tail_page.pages[0]
        tail_RID   = tail_page.pages[1].retrieve(tail_rec_ind)
        tail_indir[tail_RID] = base_indir_val
        tail_page.isPinned -= 1
        # Add new tail record to base rec indirection
        # if prev_tail_page != None:
        base_indir[rec_RID] = (tail_page.path, tail_rec_ind+1, threading.get_ident())
        # update base schema
        cpage.pages[3][rec_ind] = new_schema
        return

    '''
        Returns the values updated in this tail record (from previous tail record)
            -- by checking schema (need to update schema in this way in update)
            1,2,3,4->1,3,4,4 0110
            1,3,4,4->1,5,4,4 0100
            return [none,3,4,none]
    '''
    def get_updated_tail_cols(self, tail_page, tail_rec_ind):
        values = []
        tail_page_schema = tail_page.pages[3][tail_rec_ind]
        for i, val in enumerate(tail_page_schema):
            if val == 1:
                values.append(tail_page.pages[6+i].retrieve(tail_rec_ind))
            else:
                values.append(MAX_INT)
        return values

    # Evicting all pages in the BufferPool, with the thread_num
        # 1. Go to lock_manager and check if the key is there for a transaction
        # 2. Go to bufferpool and check if the key is there
        # 3. Write to disk
    # Release your locks on that transaction num
    def commit(self):
        self.query_obj.table.lock_manager.release_locks()


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
