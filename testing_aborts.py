from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker
from template.config import init
import threading, time
from random import choice, randint, sample, seed


'''
    - When testing aborts here, must add "thread.join()" before the return in transaction_worker.py
    - Also need to set the trans_num in query.locked_for_read/write to
        "trans_num = self.trans_num" instead of what is there currently
    - Add counter to transaction_worker.run && transaction.run params
        and uncomment self.counter = counter
'''

init()
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 8

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

transaction_workers = []
insert_transactions = []
select_transactions = []
update_transactions = []
for i in range(num_threads):
    insert_transactions.append(Transaction())
    select_transactions.append(Transaction())
    update_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    transaction_workers[i].add_transaction(insert_transactions[i])
    transaction_workers[i].add_transaction(select_transactions[i])
    transaction_workers[i].add_transaction(update_transactions[i])
worker_keys = [ {} for t in transaction_workers ]
query = Query(grades_table)

for i in range(8):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    insert_transactions[i].add_query(query.insert, *records[key])
    worker_keys[i][key] = True
    print("insert: ", records[key])

a = 0
for i in range(8):
    key = 92106429 + i
    updated_columns = [key, None, None, None, None]
    value = randint(0, 20)
    updated_columns[1] = value
    records[key][1]    = value
    a += 1
    if a == 8:
        a = 0
    update_transactions[a].add_query(query.update, key, *updated_columns)
    print("update: ", updated_columns)
    updated_columns = [None, None, None, None, None]

counter = 0

for transaction_worker in transaction_workers:
    transaction_worker.run(counter)
    counter += 1

# while threading.active_count() != 1:
while threading.active_count() > 4:
    # time.sleep(1)
    pass
