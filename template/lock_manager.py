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
