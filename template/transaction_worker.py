from template.table import Table, Record
from template.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats  = []
        self.transactions = transactions
        self.result = 0

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    def run_transaction(self):
        for i, transaction in enumerate(self.transactions):
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))


    """
    Runs a transaction
    """
    def run(self):
        threading.Threading(target=self.run_transaction).start()
