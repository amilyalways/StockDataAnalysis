import pymysql
import time
import matplotlib.pyplot as plt

# connect to db
def connect_db(host, db, username, password):
    config = {
        'host': host,
        'port': 3306,
        'user': username,
        'password': password,
        'db': db,
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    conn = pymysql.connect(**config)
    return conn

## close db
def close_db(cur, conn):
    cur.close()
    conn.close()
    return

conn = connect_db('localhost','stockresult','root','0910mysql@')
cur = conn.cursor()
'''
month_list = ["2013","201303","201304","201305","201306","201307","201308","201309","201310","201311","201312"]

for month in month_list:
    sql = "select * from stats_c_i_m where Times=" + month + " and isTop=0 order by average_revenue DESC limit 5"
    cur.execute(sql)
    result = cur.fetchall()
    print(month + ": ")
    for re in result:
        print(re)



def time_to_long(times):
    print(times[:17])

    t = times[:17]
    print(type(t))
    L = time.mktime(time.strptime(t, '%Y%m%d-%H:%M:%S'))
    print(L)
    print(times[18:])
    L = L + int(times[18:])*0.001
    print(L)

time_to_long("20130516-11:03:47 500")
'''




