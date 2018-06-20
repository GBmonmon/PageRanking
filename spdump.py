import sqlite3

conn = sqlite3.connect('spider1.sqlite')
cur = conn.cursor()

cur.execute('''SELECT COUNT(from_id) AS inbound, url, id, old_rank, new_rank FROM Pages JOIN Links
ON Pages.id = Links.to_id WHERE html is not NULL AND error is not NULL
GROUP BY id ORDER BY inbound DESC ''')
