from db import Database
from query import Query
from index import Index
from time import process_time
from random import choice, randrange, sample, randint

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
records = {}

insert_time_0 = process_time()
# for i in range(0, 10000):
# 	key = 906659671 + i
# 	query.insert(key, 93, 0, 0, 0)
# 	records[key] = [key, 93, 0, 0, 0]
# 	keys.append(key)
for i in range(0, 10000):
	key = 92106429 + randint(0, 11000)
	while key in records: # Prevents duplicate keys
		key = 92106429 + randint(0, 9000)
	records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
	query.insert(*records[key])
	keys.append(key)
	# print('inserted', records[key])
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# # Measuring Select Performance
# select_time_0 = process_time()
# for i in range(0, 10000):
#     query.select(choice(keys), 0, [1, 1, 1, 1, 1])
# select_time_1 = process_time()
# print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# # Measuring Aggregate Performance
# agg_time_0 = process_time()
# for i in range(0, 10000, 100):
# 	# rand = randrange(0, 5)
# 	result = query.sum(i, 100, randrange(0, 5))
# agg_time_1 = process_time()
# print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

agg_time_0 = process_time()
for c in range(0, grades_table.num_columns):
	for i in range(0, 20):
		r = sorted(sample(range(0, len(keys)), 2))
		column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
		result = query.sum(keys[r[0]], keys[r[1]], c)
		# if column_sum != result:
		#     print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
		# else:
		#     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
