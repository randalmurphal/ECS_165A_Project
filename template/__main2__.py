from db import Database
from query import Query
from time import process_time
from random import choice, randrange, randint, sample
import numpy as np

# Student Id and 4 grades
db           = Database()
grades_table = db.create_table('Grades', 5, 0)
query        = Query(grades_table)
keys         = []
records      = {}
start_key    = 92106429
num_iters    = 10000
testing      = True

""" INSERT TEST """

insert_time_0 = process_time()

for i in range(0, num_iters):
    key = start_key + randint(0, num_iters-1)
    while key in records: # Prevents duplicate keys
        key = start_key + randint(0, num_iters-1)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    keys.append(key)
    if testing:
        print('inserted', records[key])

insert_time_1 = process_time()

""" SELECT TEST """

select_time_0 = process_time()

for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
    elif testing:
        print('select on', key, ':', record.columns)

select_time_1 = process_time()

""" UPDATE TEST """

update_time_0 = process_time()

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
        elif testing:
            print('update on', original, 'and', updated_columns, ':', record.columns)
        updated_columns[i] = None

update_time_1 = process_time()

""" SUM TEST """

agg_time_0 = process_time()

for i in range(0, num_iters, 100):
    c = randrange(0, grades_table.num_columns)
    r = sorted(sample(range(0, len(keys)), 2))

    start = start_key + i
    end   = start + 99

    start_ind = 0
    end_ind   = 0
    for i, val in enumerate(keys):
        if val == start:
            start_ind = i
        elif val == end:
            end_ind = i
            break

    column_sum = sum(map(lambda key: records[key][c], keys[start_ind: end_ind+1]))
    result = query.sum(keys[start_ind], keys[end_ind], c)
    if column_sum != result:
        print('sum error on [', keys[start_ind], ',', keys[end_ind], ']: ', result, ', correct: ', column_sum)
    elif testing:
        print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)

agg_time_1 = process_time()

""" DELETE TEST """
import time

delete_time_0 = process_time()
for i in range(0, num_iters):
    location = query.table.key_dict[start_key + i]
    p_r, b_p, p, r = location
    base_page = query.table.page_directory[p_r].range[0][b_p].pages
    indirection = base_page[0]
    rid = base_page[1][p].retrieve(r)
    query.delete(start_key + i)
    schema_ind = 512 * p + r
    schema = base_page[3][schema_ind]
    in_indirection = rid in indirection.keys()
    schema_zeros = schema.all() == np.zeros(grades_table.num_columns).all()
    correct = schema_zeros and in_indirection
    if not correct:
        print("Delete error on", key, ":", record.columns)
    elif testing:
        record = query.select(start_key + i, 0, [1, 1, 1, 1, 1])[0]
        print("Delete on", key, ":", record.columns)
delete_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
print("Aggregate 10k of 100 record batch took:\t\t", agg_time_1 - agg_time_0)
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
