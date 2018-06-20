import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')


cur.execute('''SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1''')
row = cur.fetchone()
if row is not None:
    print('recrawling, or start a new DB.')
else:
    starturl = input('Input the web url:')
    if len(starturl)<1: starturl = 'http://www.dr-chuck.com/'
    if starturl.endswith('/'): starturl = starturl[:-1]
    web = starturl
    print(web)
    if starturl.endswith('.htm') or starturl.endswith('.html'):
        pos = starturl.rfind('/')
        web = starturl[:pos]
        print(web)

    if len(web)>0:
        cur.execute('INSERT OR IGNORE INTO Webs(url)VALUES(?)',(web,))
        cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank)VALUES(?, NULL, 1)',(starturl,))
        conn.commit()

cur.execute('SELECT url FROM Webs')
Webs = list()
for row in cur:
    Webs.append(row[0])
print(Webs)

many = 0
while True:
    if many<1:
        sval = input('How many pages:')
        if len(sval)<1: break
        many = int(sval)
    many = many-1
    cur.execute('''SELECT id, url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1''')
    row = cur.fetchone()
    try:
        from_id = row[0]
        url = row[1]
        print(from_id, url, end=' ')

    except:
        print('There are no unretrieved url in DB!')

        break

    try:
        uh = urlopen(url, context = ctx)
        if uh.getcode()!=200:
            print('-----web site error-----:',uh.getcode())
            cur.execute('UPDATE Pages SET error = ? WHERE url = ?',(uh.getcode(),url))
            conn.commit()
        if uh.info().get_content_type() != 'text/html':
            print('right type:','text/html','wrong type:',uh.info().get_content_type())
            cur.execute('DELETE FROM Pages WHERE url = ?',(url,))
            continue

        html = uh.read()
        print('('+str(len(html))+')',end = ' ')
        soup = BeautifulSoup(html,'html.parser')
    except KeyboardInterrupt:
        print('User interrupt!!')
    except:
        print('fail to retrieve')
        cur.execute('UPDATE Pages SET error = -1 WHERE url = ?',(url,))
        continue

    cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank)VALUES(?, NULL, 1)',(url,))
    cur.execute('UPDATE Pages SET html = ? WHERE url = ?',(memoryview(html),url))
    conn.commit()


    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href',None)
        if href == None: continue
        if href.endswith('/'): href = href[:-1]
        ipos = href.find('#')
        if ipos>1: href = href[:ipos]
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
        up = urlparse(href)
        if len(up.scheme)<1:
            href = urljoin(url, href)
        if len(href)<1: continue

        found = True
        for web in Webs:
            if href.startswith(web):
                found = False
                break

        if found==True  : continue


        cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank)VALUES(?, NULL, 1)',(href,))
        conn.commit()
        count = count +1


        cur.execute('SELECT id FROM Pages WHERE url = ? LIMIT 1',(href,))
        try:
            row = cur.fetchone()
            to_id = row[0]
        except:
            print('No links id!!')
            continue
        cur.execute('INSERT OR IGNORE INTO Links(from_id, to_id)VALUES(?,?)',(from_id, to_id))
        conn.commit()
    print('links:',count)
