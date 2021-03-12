from template.table import Table, Record
from template.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.stats  = []
        self.transactions = []
        self.result = 0
        self.query_obj = None
        # self.counter = 0

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    def run_transaction(self):
        for i, transaction in enumerate(self.transactions):
            # each transaction returns True if committed or False if aborted
            # print("THIS IS THE ", i, " TRANSACTION FOR THIS WORKER (should only be 1)")
            self.stats.append(transaction.run(self.query_obj))
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
        return

    """
    Runs a transaction
    """
    def run(self):
        # self.counter = counter
        self.query_obj = self.transactions[0].queries[0][0].__self__
        thread = threading.Thread(target=self.run_transaction)
        thread.start()
        # thread.join()
        return
