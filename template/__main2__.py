from db import Database
from query import Query
from time import process_time
from random import choice, randrange, randint, sample
import numpy as np

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
records = {}
start_key = 92106429
num_iters = 10000
testing = True

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
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

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
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

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
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

""" SUM TEST """

agg_time_0 = process_time()

# keys_2 = sorted(list(records.keys()))
keys_2 = list(records.keys())
# for c in range(0, grades_table.num_columns):
for i in range(0, num_iters, 100):
    c = randrange(0, grades_table.num_columns)
    r = sorted(sample(range(0, len(keys_2)), 2))
    column_sum = sum(map(lambda key: records[key][c], keys_2[r[0]: r[1] + 1]))
    result = query.sum(keys_2[r[0]], keys_2[r[1]], c)
    if column_sum != result:
        print('sum error on [', keys_2[r[0]], ',', keys_2[r[1]], ']: ', result, ', correct: ', column_sum)
    elif testing:
        print('sum on [', keys_2[r[0]], ',', keys_2[r[1]], ']: ', column_sum)

agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t\t", agg_time_1 - agg_time_0)

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
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)


# insert_time_0 = process_time()
# for i in range(0, 10000):
#     query.insert(906659671 + i, 93, 0, 0, 0)
#     keys.append(906659671 + i)
# insert_time_1 = process_time()

# print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)
#
# # Measuring update Performance
# update_cols = [
#     [randrange(0, 100), None, None, None, None],
#     [None, randrange(0, 100), None, None, None],
#     [None, None, randrange(0, 100), None, None],
#     [None, None, None, randrange(0, 100), None],
#     [None, None, None, None, randrange(0, 100)],
# ]
#
# update_time_0 = process_time()
# for i in range(0, 10000):
#     query.update(choice(keys), *(choice(update_cols)))
# update_time_1 = process_time()
# print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
#
# # Measuring Select Performance
# select_time_0 = process_time()
# for i in range(0, 10000):
#     query.select(choice(keys),0 , [1, 1, 1, 1, 1])
# select_time_1 = process_time()
# print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
#
# # Measuring Aggregate Performance
# agg_time_0 = process_time()
# for i in range(0, 10000, 100):
#     start_value = 906659671 + i
#     end_value = start_value + 100
#     result = query.sum(start_value, end_value - 1, randrange(0, 5))
# agg_time_1 = process_time()
# print("Aggregate 10k of 100 record batch took:\t\t", agg_time_1 - agg_time_0)
#
# # Measuring Delete Performance
# delete_time_0 = process_time()
# for i in range(0, 10000):
#     query.delete(906659671 + i)
# delete_time_1 = process_time()
# print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
