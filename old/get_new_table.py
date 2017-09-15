import pymysql
import time

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

#clear_table
def clear_table(cur,conn,tablename):
    sql = "delete from " + tablename
    cur.execute(sql)
    conn.commit()

def find_day(cur,day_list,tablename):
    candidate_day = []
    for day in day_list:
        print(day)
        sql = "select * from " + tablename + " where Times like '" + day +  "%' limit 1"
        print(sql)
        cur.execute(sql)
        result = cur.fetchall()
        print(result)
        if len(result) > 0:
            candidate_day.append(day)
            print(candidate_day)
    return candidate_day

def stats_day(cur,day_list, tablename):
    Mu_stats = {}
    LossingStopUp_stats = {}
    for day in day_list:
        sql = "select avg(Mu), std(Mu), avg(LossingStopUp), std(LossingStopUp) from " + tablename + " where Times like '" + day + "%'"
        cur.execute(sql)
        result = cur.fetchall()[0]
        print(result)
        M_value = {'avg':result['avg(Mu)'], 'std':result['std(Mu)']}
        Mu_stats.setdefault(day,M_value)
        L_value = {'avg':result['avg(LossingStopUp)'], 'std':result['std(LossingStopUp)']}
        LossingStopUp_stats.setdefault(day,L_value)
    print("Mu Stats")
    print("day           avg            std")
    for day in Mu_stats:
        s = day + "    " + str(Mu_stats[day]['avg']) + "     " + str(Mu_stats[day]['std'])
        print(s)

    print("LossingStopUp Stats")
    print("day           avg            std")
    for day in LossingStopUp_stats:
        s = day + "    " + str(LossingStopUp_stats[day]['avg']) + "     " + str(LossingStopUp_stats[day]['std'])
        print(s)


def time_to_long(times):
    t = time.mktime(time.strptime(times[:17], '%Y%m%d-%H:%M:%S'))
    t = t * 1000 + float(times[18:])
    return t
'''
def is_need_record(before_times,now_times,offset):
    before_t = time_to_long(before_times)
    now_t = time_to_long(now_times)

    if (now_t - before_t) == offset:
        return True
    else:
        return False
'''

def is_need_record(now_offset,offset):
    if now_offset == offset:
        return True
    else:
        return False

def create_table(cur, conn,tablename,features):

    sql1 = "create table if not exists " + tablename + " (" + features + " )"
    cur.execute(sql1)
    conn.commit()

#clear_table
def clear_table(cur,conn,tablename):
    sql = "delete from " + tablename
    cur.execute(sql)
    conn.commit()

def insert_into_db(cur,conn,re,tablename,row_id):
    sql = "insert into "+ tablename + " values('" + str(re['Times']) + "', " + str(re['Mu']) + ", " + str(re['Sigma'])
    sql += ", " + str(re['LossingStopUp']) + ", " + str(re['LossingStopDown']) + ", " + str(re['ComputeLantency'])
    sql += ", " + str(re['IntervalNum']) + ", " + str(re['MuUpper']) + ", " + str(re['MuLower'])  + ", " + str(row_id) + " )"
    #print(sql)
    cur.execute(sql)
    conn.commit()
    return


def get_new_table(cur,conn, from_tablename, to_tablename):

    start = 0
    record_offset = 100000
    sql1 = "select count(*) from " + from_tablename
    cur.execute(sql1)
    record_num = cur.fetchall()[0]['count(*)']

    final = 0
    row_id = 0
    while start < record_num:

        if final >= 2:
            break
        print("start:" + str(start))
        sql = "select * from " + from_tablename + " limit " + str(start) + ", " + str(record_offset)
        cur.execute(sql)
        result = cur.fetchall()

        offset = result[0]['IntervalNum'] * 0.05 - 1
        print(offset)
        count = 1
        insert_into_db(cur, conn, result[0], to_tablename,row_id)
        row_id += 1

        now_offset = 0

        for re in result[1:]:
            if is_need_record(now_offset, offset):
                #print(str((record_offset - count)))
                #print(str((offset/10000)))
                if (record_offset - count) <= offset:
                    before_start = start
                    start += count
                    print("start:" + str(start) + " count: " + str(count))
                    break
                #print(re)
                insert_into_db(cur, conn, re, to_tablename,row_id)
                row_id += 1
                #before_times = re['Times']
                offset = re['IntervalNum'] * 0.05 - 1
                now_offset = 0
                #print(offset)

            count += 1
            now_offset += 1
            #print(count)
        print("count:" + str(count))
        if start == 18398938:
            final += 1












conn = connect_db('localhost','stockresult','root','0910mysql@')
cur = conn.cursor()

'''
day_list = []
for i in range(8,10):
    day_list.append("2013100"+str(i))
for i in range(10,21):
    day_list.append("201310" + str(i))
candidate_day = find_day(cur,day_list,"InnerValue")
'''
'''
day_list = ['20131008', '20131009', '20131010', '20131011', '20131014', '20131015', '20131016', '20131017', '20131018']

stats_day(cur,day_list,"NoShift_InnerValue")



features = "Times varchar(50), " \
           "Mu double, " \
           "Sigma double," \
           "LossingStopUp double," \
           "LossingStopDown double," \
           "ComputeLantency int(11), " \
           "IntervalNum int(11), " \
           "MuUpper double," \
           "MuLower double," \
           "RowId int(11)," \
           "PRIMARY KEY (`RowId`)" \

create_table(cur,conn,"NoShift_InnerValue",features)
clear_table(cur,conn,"NoShift_InnerValue")
get_new_table(cur,conn,"InnerValue","NoShift_InnerValue")
'''
#sql = "create table no_shift select Times,Mu,Sigma,LossingStopUp,LossingStopDown,IntervalNum,MuUpper,MuLower "
#sql += "from stockresult.InnerValue_no_shift group by Times,Mu,Sigma,LossingStopUp,LossingStopDown,IntervalNum,MuUpper,MuLower having count(*) < 2"

#cur.execute(sql)
'''
sql = "create table tradeinfos_InnerValue_2days (" \
      "select tradeinfos2day.Times, tradeinfos2day.LastPrice, tradeinfos2day.isOpen,tradeinfos2day.isLong,tradeinfos2day.ComputeLantency,tradeinfos2day.IntervalNum," \
      "tradeinfos2day.MuUpper, tradeinfos2day.MuLower, innervalue2day.Mu,innervalue2day.Sigma,innervalue2day.LossingStopUp,innervalue2day.LossingStopDown" \
      " from tradeinfos2day inner join innervalue2day on tradeinfos2day.Times=innervalue2day.Times and tradeinfos2day.ComputeLantency=innervalue2day.ComputeLantency and" \
      " tradeinfos2day.IntervalNum=innervalue2day.IntervalNum and tradeinfos2day.MuUpper=innervalue2day.MuUpper and tradeinfos2day.MuLower=innervalue2day.MuLower)"

print(sql)
'''
#clear_table(cur,conn,"InnerValue2day")
#clear_table(cur,conn,"tradeinfos2day")
'''
sql = "select distinct Mu,LossingStopUp, Sigma from InnerValue2day where Times like '20130516%' and ComputeLantency=5 and IntervalNum=120 and MuUpper=0.0"
#sql = "select * from InnerValue2day limit 100"
#sql = "select * from InnerValue2day where Times='20130516-09:18:00 0 0'"
cur.execute(sql)
print("InnerValue-----------------------------------------------------")
result = cur.fetchall()
for re in result:
    print(re)

sql = "select * from tradeinfoswithmu where Times= '20130516-09:51:00 500'"

cur.execute(sql)
result = cur.fetchall()
print("Tradeinfoswithmu-----------------------------------------------------")
print(result)

for re in result:
    print(re)

sql = "select * from tradeinfos2day where ComputeLantency=10 and IntervalNum=60 and MuUpper=0.0"
#sql = "select * from tradeinfos2day limit 100"
cur.execute(sql)
result = cur.fetchall()
print("tradeinfos2day-----------------------------------------------------")
print(result)
for re in result:
    print(re)
#conn.commit()

sql = "SELECT * FROM innervalue2day group by Times, ComputeLantency,IntervalNum,MuUpper,MuLower having count(*) >1"
cur.execute(sql)
result = cur.fetchall()
for re in result:
    print(re)
'''

sql = "create table InnerValue_revenue (" \
      "select revenue.Times, revenue.LastPrice, revenue.isOpen,revenue.isLong,revenue.ComputeLantency,revenue.IntervalNum," \
      "revenue.MuUpper, revenue.MuLower, revenue.Revenue, NoShift_InnerValue.Mu,NoShift_InnerValue.Sigma,NoShift_InnerValue.LossingStopUp,NoShift_InnerValue.LossingStopDown" \
      " from revenue inner join NoShift_InnerValue " \
      "on revenue.Times like '201310%' and revenue.Times=NoShift_InnerValue.Times and revenue.ComputeLantency=NoShift_InnerValue.ComputeLantency and" \
      " revenue.IntervalNum=NoShift_InnerValue.IntervalNum and revenue.MuUpper=NoShift_InnerValue.MuUpper and revenue.MuLower=NoShift_InnerValue.MuLower)"

print(sql)
cur.execute(sql)
conn.commit()
close_db(cur, conn)