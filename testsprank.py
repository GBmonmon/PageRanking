import sqlite3

conn = sqlite3.connect('spider1.sqlite')
cur = conn.cursor()

from_ids = list()
cur.execute('SELECT DISTINCT(from_id) FROM Links')
for row in cur:
    from_ids.append(row[0])

to_ids = list()
links = list()
cur.execute('SELECT DISTINCT from_id, to_id FROM Links')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id not in from_ids: continue
    if from_id == to_id: continue
    if to_id not in from_ids: continue
    links.append(row)
    if to_id not in to_ids: to_ids.append(to_id)

prev_ranks = dict()
for node in from_ids:
    cur.execute('SELECT new_rank FROM Pages WHERE id = ?',(node,))
    row = cur.fetchone()
    prev_ranks[node] = row[0]


if len(prev_ranks) < 1:
    print('no date in DB!')
    quit()


many = 1
sval = input('How many iterations:')
if len(sval)>0: many = int(sval)

for i in range(many):
    next_ranks = dict()
    total = 0.0
    for node, old_rank in list(prev_ranks.items()):
        next_ranks[node] = 0
        total = total + old_rank

    for node, old_rank in list(prev_ranks.items()):
        give_ids = list()

        for from_id, to_id in links:
            if from_id != node: continue
            if to_id not in to_ids: continue
            give_ids.append(to_id)
        if len(give_ids)<1: continue
        amount = old_rank/len(give_ids)

        for toid in give_ids:
            next_ranks[toid] = next_ranks[toid] + amount

    newtot = 0
    for node, new_rank in list(next_ranks.items()):
        newtot = newtot + new_rank
    #print('111',newtot)
    evap = (total - newtot)/len(next_ranks)

    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtot = 0
    for node, new_rank in list(next_ranks.items()):
        newtot = newtot + new_rank
    #print('222newtot',newtot)
    totdiff = 0
    for node, old_rank in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank - new_rank)
        totdiff = totdiff + diff
    avediff = totdiff / len(prev_ranks)

    print(avediff)

    prev_ranks = next_ranks

cur.execute('UPDATE Pages SET old_rank = new_rank')
for node, new_rank in list(next_ranks.items()):
    cur.execute('UPDATE Pages SET new_rank = ? WHERE id = ?',(new_rank,node))
conn.commit()
cur.close()
