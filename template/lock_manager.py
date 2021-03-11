class LockManager():
    def __init__(self):
        self.lock_recs = {}

    def release_locks(self, thread_num):
        pass

    def add_lock(self, key, lock_type, thread_num):
        # 1. Add key:(type_of_lock,loc_of_rec,thread_num)
        type_of_lock = lock_type #This is for locking
        # loc_of_rec = rec_loc #This is for deleting in Bufferpool, key:loc in key_dict
        thread_num = thread_num #This is for deleteing using the locked_recs
        self.lock_recs[key] = (type_of_lock,thread_num)

    def check_lock_dict(self, key, lock_type, thread_num):
        pass

    def add_read_lock(self, key, lock_type, thread_num):
        pass

'''
    """
    Update a record with specified key and columns
    Returns True if update is succesful
    Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # get base page & pin -- adds to bufferpool
        base_page = self.get_from_disk(key=key)
        base_page.isPinned = True
        # Get tail page -- adds to bufferpool
        tail_page = self.get_tail_page(base_page, key, False, *columns)
        self.update_tail_page(base_page, tail_page, key, *columns)
        base_page.isPinned = False
        base_page.dirty    = True
        if base_page.full():
            if tail_page.path not in self.buffer_pool.merge_tails:
                self.buffer_pool.merge_tails.append(tail_page.path)
            if base_page.path not in self.buffer_pool.merge_bases:
                self.buffer_pool.merge_bases.append(base_page.path)
        self.merge_count += 1
        if self.merge_count == self.merge_frequency:
            merge_thread = threading.Thread(target=self.buffer_pool.merge())
            merge_thread.start()
        return True

    ### ******* Update Helpers ******* ###

        Updates a tail record for a new update on a base record
        - On first update for any column, add snapshot with the update
        tail_page.isPinned += 1
        rec_ind             = self.buffer_pool.meta_data.key_dict[key][3]
        base_rec_RID        = base_page.pages[1].retrieve(rec_ind)

        if base_rec_RID == -1:
            # If record that is retrieved is locked
            return False
        base_indirection    = base_page.pages[0]
        new_schema = copy.copy(base_page.pages[3][rec_ind])
        old_schema = base_page.pages[3][rec_ind]

        query_cols = self.update_schema(new_schema, *columns)
        thread_num = threading.get_ident()
        tail_rec_ind = tail_page.num_records
        tail_path    = tail_page.path
        prev_tail_values = []
        # check if base record has been updated previously, if it has then grab the values from the previous update.
        if base_rec_RID in base_indirection.keys():
            prev_tail_values = self.get_prev_tail(key, rec_ind, base_rec_RID, base_page, *columns) # base_rec_RID, base_indirection)
        else:
            prev_tail_values = [MAX_INT]*self.table.num_columns
        # If first update for any column, save snapshot
        if not np.array_equal(old_schema, new_schema):
            buffer_lock.acquire()
            # if tail_page.full():
            #     tail_page.isPinned -= 1
            #     pr_num    = base_page.path.split("/")[4][2:]
            #     tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
            #     tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
            #     tail_page.isPinned += 1
            # success = self.create_snapshot(old_schema, new_schema, rec_ind, base_page, tail_page)
            # if not success:
            #     return False
            # if full after creating snapshot
            if tail_page.full():
                tail_page.isPinned -= 1
                pr_num    = base_page.path.split("/")[4][2:]
                tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned += 1
            # else:
            if not rec_ind in base_indirection.keys():
                base_indir_val = tail_path, tail_rec_ind, thread_num
            else:
                base_indir_val = base_indirection[rec_ind]
            # tail_rec_ind += 1
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, tail_rec_ind, query_cols, base_indir_val, *columns)
            # self.buffer_pool.addConceptualPage(tail_page)
            buffer_lock.release()
        else: #If we don't create a snapshot
            if not rec_ind in base_indirection.keys():
                base_indir_val = tail_path, tail_rec_ind, thread_num
            else:
                base_indir_val = base_indirection[rec_ind]
            # If full, create new tail page
            if tail_page.full():
                tail_page.isPinned -= 1
                pr_num    = base_page.path.split("/")[4][2:]
                tail_path = './template/%s/'%(self.table.path) + self.table.name + '/PR' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                buffer_lock.acquire()
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned += 1
                base_indir_val = base_indirection[rec_ind]
                # Update tail page to most updated values (including previous)
                tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, tail_rec_ind, query_cols, base_indir_val, *columns)
                buffer_lock.release()
            else:
                # Update tail page to most updated values (including previous)
                buffer_lock.acquire()
                tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, tail_rec_ind, query_cols, base_indir_val, *columns)
                tail_page.isPinned += 1
                # self.buffer_pool.addConceptualPage(tail_page)
                buffer_lock.release()
        if tail_page == None:
            return False
        tail_rec_ind = tail_page.num_records
        tail_path = tail_page.path
        base_page.pages[3][rec_ind]      = new_schema
        base_page.pages[0][base_rec_RID] = tail_path, tail_rec_ind, thread_num
        tail_page.isPinned -= 1
        return True
    def update_tail_page(self, base_page, tail_page, key, *columns):
        tail_page.isPinned = True
        rec_ind       = self.buffer_pool.meta_data.key_dict[key][3]
        base_rec_RID       = base_page.pages[1].retrieve(rec_ind)
        base_indirection   = base_page.pages[0]
        new_schema = copy.copy(base_page.pages[3][rec_ind])
        old_schema = base_page.pages[3][rec_ind]
        # query_cols = self.get_query_cols(key, *columns)
        query_cols = self.update_schema(new_schema, *columns)
        prev_tail_values = []
        # check if base record has been updated previously, if it has then grab the values from the previous update.
        if base_rec_RID in base_indirection.keys():
            prev_tail_values = self.get_prev_tail(key, rec_ind, base_rec_RID, base_page, *columns) # base_rec_RID, base_indirection)
        # If first update for any column, save snapshot
        if not np.array_equal(old_schema, new_schema):
            self.create_snapshot(old_schema, new_schema, rec_ind, base_page, tail_page)
            # if full after creating snapshot
            if tail_page.full():
                tail_page.isPinned = False
                tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned = True
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, *columns)
        else: #If we don't create a snapshot
            # If full, create new tail page
            if tail_page.full():
                tail_page.isPinned = False
                pr_num    = base_page.path.split("/")[3][2:]
                tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
                tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                tail_page.isPinned = True
            tail_page = self.create_tail(new_schema, prev_tail_values, tail_page, *columns)

        base_page.pages[3][rec_ind] = new_schema
        tail_rec_ind     = tail_page.num_records - 1
        tail_path        = tail_page.path
        base_page.pages[0][base_rec_RID] = tail_path, tail_rec_ind
        tail_page.isPinned = False
        return True

    
        Writes snapshot to tail page
    
    def create_snapshot(self, old_schema, new_schema, rec_ind, base_page, tail_page):
        snapshot_col = []
        # Goes through to check which schema different from last
        for i in range(len(old_schema)):
            if new_schema[i] != old_schema[i]:
                snapshot_col.append(1)
            else:
                snapshot_col.append(0)
        # makes sure to update RID and tail page record num for the snapshot
        for i, val in enumerate(snapshot_col):
            if val == 1:
                tail_page.pages[i+6].write(base_page.pages[i+6].retrieve(rec_ind))
            else:
                tail_page.pages[i+6].write(MAX_INT)
        tail_page.num_records += 1
        self.buffer_pool.meta_data.tailRID_count += 1

    
        Writes tail record to tail page
    
    def create_tail(self, new_schema, prev_tail_values, tail_page, *columns):
        for i, val in enumerate(new_schema):
            # If page has update at column i
            if val == 1:
                if columns[i] == None:
                    # Set value to prev tail page value
                    tail_page.pages[i+6].write(prev_tail_values[i])
                else:
                    # Set value to updated value
                    tail_page.pages[i+6].write(columns[i])
            else:
                # Set value to None/MaxInt
                tail_page.pages[i+6].write(MAX_INT)
        # UPDATE schema_col
        tail_page.num_records += 1
        self.buffer_pool.meta_data.tailRID_count += 1
        return tail_page
    
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int   # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Used through the bufferpool
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    

    
        Updates schema & returns the query columns to index updated cols
    
    def update_schema(self, base_schema, *columns):
        # Get query cols from columns
        query_cols = []
        for i, col in enumerate(columns):
            if col != None:
                query_cols.append(1)
            else:
                query_cols.append(0)
        # update the schema according to query cols
        for i, col in enumerate(query_cols):
            if col == 1:
                base_schema[i] = 1
        return query_cols

    
        Get tail page, even if it is full
        - Assumes base_rid in base_indirection already
        - Returns values so it is easier to process
    
    def get_prev_tail(self, key, rec_ind, base_rid, base_page, *columns):
        base_rec_RID     = base_page.pages[1].retrieve(rec_ind)
        base_indirection = base_page.pages[0]
        prev_tail_path, prev_tail_rec_ind = base_indirection[base_rid]
        prev_tail_values = []
        prev_tail_pg     = self.get_tail_page(base_page, key, True, *columns)
        prev_tail_pg.isPinned = True
        for i in range(len(columns)):
            prev_tail_values.append(prev_tail_pg.pages[6+i].retrieve(prev_tail_rec_ind))
        prev_tail_pg.isPinned = False
        return prev_tail_values

    def get_tail_page(self, base_page, key, prev, *columns):
        base_page.isPinned = True
        indirection  = base_page.pages[0]
        base_rec_ind = self.buffer_pool.meta_data.key_dict[key][3] # record num in bp
        base_rec_RID = base_page.pages[1].retrieve(base_rec_ind)   # RID of base record
        pr_num       = base_page.path.split('/')[4]                # keep same PR as base page for tail
        # If tail page exists, get values from indirection. Else create new tail path
        if base_rec_RID in indirection.keys():
            tail_path, tail_rec_ind = indirection[base_rec_RID]
        else:
            tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count//512))
        # if tail not in buffer, get from file and add to buffer_pool. Else just grab from buffer_pool
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            # The first tail page for the table should create a new one
            if not self.buffer_pool.first:
                if self.tail_in_indirection(tail_path, indirection):
                    tail_page = self.get_from_disk(key, tail_path)
                    # prev = false for new tail page full & not looking for prev tail values
                    if tail_page.full() and prev == False:
                        tail_page = self.new_tail_page(pr_num, *columns)
                else:
                    tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
                    self.add_meta(tail_page)
            else:
                # the first page -- create & add to buffer_pool
                self.buffer_pool.first = False
                tail_page = self.new_tail_page(pr_num, *columns)
        else:
            if tail_page.full() and prev == False:
                tail_page = self.new_tail_page(pr_num, *columns)
        base_page.isPinned = False
        return tail_page

    
        Gets a page from disk
        - if no path, get path from key
    
    def get_from_disk(self, key=None, path=None):
        # If no path, get path for base page from key
        if path == None:
            if key == None:
                raise TypeError("Key value is None: get_from_disk()")
            location = self.buffer_pool.meta_data.key_dict[key]
            path     = './template/ECS165/'+self.table.name+'/PR'+str(location[0])+'/BP'+str(location[1])
        # if not in buffer, grab from disk
        cpage, is_in_buffer = self.in_buffer(path)
        if not is_in_buffer:
            with open(path, 'rb') as db_file:
                cpage = pickle.load(db_file)
            self.buffer_pool.addConceptualPage(cpage)
        return cpage

    
        Creates new tail page from meta_data path & add to buffer_pool
    
    def new_tail_page(self, pr_num, *columns):
        tail_path = './template/ECS165/' + self.table.name + '/' + str(pr_num) + '/TP' + str((self.buffer_pool.meta_data.tailRID_count // 512))
        tail_page, is_in_buffer = self.in_buffer(tail_path)
        if not is_in_buffer:
            tail_page = self.buffer_pool.createConceptualPage(tail_path, True, *columns)
        self.add_meta(tail_page)
        return tail_page
'''
