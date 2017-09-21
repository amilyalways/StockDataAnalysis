#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import division
import pymysql
from Utility.DB import DB
#import matplotlib.pyplot as plt


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

# select data from db
def select_from_db(cur,sql):
    cur.execute(sql)
    result = cur.fetchall()
    return result

# insert data into db
def insert_into_db(cur,conn, tablename, values):
    sql = "insert into " + tablename + " values(" + values + ") "
    cur.execute(sql)
    conn.commit()
    return

def ComputeRevenue(price1,price2,isLong):
    if isLong == 0:
        t = -1
    else:
        t = 1
    return (price2-price1)*t

#create tevenue table
def create_table(cur, conn,tablename,features):

    sql1 = "create table if not exists " + tablename + " (" + features + " )"
    cur.execute(sql1)
    conn.commit()

def drop_table(cur, conn, tablename):
    sql =  "drop table " + tablename
    cur.execute(sql)
    conn.commit()

# write Revenue to db
def write_revenue_db(cur, conn,month_list,from_table, to_table):

    offset = 100000
    sql2 = "select count(*) from " + str(from_table)
    cur.execute(sql2)
    record_num = cur.fetchall()[0]['count(*)']
    start = 0
    print(record_num)


    while start < record_num:
        print start
        sql3 = "select * from "+ str(from_table) + " limit " + str(start) + ", " + str(offset)
        cur.execute(sql3)
        result = cur.fetchall()

        for re in result:
            if re['Times'][0:8] not in month_list:
                month_list.setdefault(re['Times'][0:8], 1)
            else:
                month_list[re['Times'][0:8]] += 1
            if re['isOpen'] == 0:
                revenue = ComputeRevenue(BeforePrice, re['LastPrice'], re['isLong'])
                '''
                sql4 = "insert into " + str(to_table) + " (Day,Times,LastPrice, isOpen,isLong,ComputeLantency," \
                       "IntervalNum, InMuUpper, lnLastPriceThreshold, Revenue ) values( '"
                sql4 += str(re['Times'][:8]) + "', '" + str(re['Times']) + "', " + str(re['LastPrice']) + ", " + str(re['isOpen'])
                sql4 += ", " + str(re['isLong']) + ", " + str(re['ComputeLantency']) + ", " + str(re['IntervalNum']) + ", "
                sql4 += str(re['InMuUpper']) + ", " + str(re['lnLastPriceThreshold']) + ", "+str(revenue) + ") "
                '''
                part1 = ""
                part2 = ""
                for ele_re in re:
                    part1 += ele_re + ","
                    if ele_re == "Times" or ele_re == "CreateTime" or ele_re == "TheKey":
                        part2 += "'" + str(re[ele_re]) + "',"
                    else:
                        part2 += str(re[ele_re]) + ","

                part1 += "Revenue"
                part2 += str(revenue)

                sql4 = "insert into " + str(to_table) + " ( " + part1 + " ) values( " + part2 + " )"

                cur.execute(sql4)

            BeforePrice = re['LastPrice']


        conn.commit()
        start = start + offset

# update statistic info
def update_statistic(dict,key, revenue,win, lose, tie):
    value = {'count':1, 'win':win, 'lose': lose, 'tie': tie, 'totalRevenue': revenue, 'averRevenue': revenue }
    if key in dict:
        dict[key]['win'] += win
        dict[key]['lose'] += lose
        dict[key]['tie'] += tie
        dict[key]['count'] += 1
        dict[key]['totalRevenue'] += revenue
        dict[key]['averRevenue'] = dict[key]['totalRevenue']/dict[key]['count']
    else:
        dict.setdefault(key,value)
    return

def printResult(dict):
    for key in dict:
        print(str(key) + " " + str(dict[key]['totalRevenue']) +" " + str(dict[key]['averRevenue']) +" " + str(dict[key]['count']))
    return

def write_stats_db(cur,conn,feature,feature_value,time,range,total_revenue,averge_revenue,count,win_percent,lose_percent,tie_percent):

    values = str(feature_value) + ", "
    if feature == "ComputeLantency":
        tablename = "stats_computelantency"
    elif feature == "IntervalNum":
        tablename = "stats_intervalnum"
    elif feature == "MuUpper":
        tablename = "stats_muupper"
    elif feature == "C_I_M":
        tablename = "stats_c_i_m"
        values = "'" + str(feature_value) + "', "
    elif feature == "Mu":
        tablename = "stats_Mu"
    elif feature == "Sigma":
        tablename = "stats_Sigma"
    elif feature == "LossingStopUp":
        tablename = "stats_LossingStopUp"
    else:
        tablename = "stats"
        values = ""

    values += str(time) + ", "+ str(range) + ", " + str(total_revenue) + ", " + str(averge_revenue) + ", " + str(count)
    values += ", " + str(win_percent) + ", " + str(lose_percent) + ", " + str(tie_percent)
    print(values)

    insert_into_db(cur,conn,tablename,values)

def write_dict_db(dict,cur,conn,feature,time,range):
    for key in dict:
        r = dict[key]
        write_stats_db(cur,conn,feature,key,time,range,r['totalRevenue'], r['averRevenue'],r['count'],r['win']/r['count'],r['lose']/r['count'],r['tie']/r['count'])



def statistic(cur,conn,sql,record_num,time,range):
    statistics_ComputeLantency = {}
    statistics_IntervalNum = {}
    statistics_MuUpper = {}
    statistics_C_I_M = {}

    win_count = 0
    lose_count = 0
    tie_count = 0
    count = 0
    total_revenue = 0
    win_revenue = 0
    lose_revenue = 0

    if range == "0":
        offset = 100000

        start = 0
        print("isTop:")
        print(range)

        while start < record_num:
            sql3 = sql + " limit " + str(start) + ", " + str(offset)
            cur.execute(sql3)
            result = cur.fetchall()
            print("start:" + str(start))

            for re in result:
                revenue = re['Revenue']
                if revenue > 0:
                    win = 1
                    lose = 0
                    tie = 0
                    win_count += 1
                    win_revenue += revenue
                else:
                    if revenue < 0:
                        win = 0
                        lose = 1
                        tie = 0
                        lose_count += 1
                        lose_revenue += revenue
                    else:
                        win = 0
                        lose = 0
                        tie = 1
                        tie_count += 1
                '''
                update_statistic(statistics_ComputeLantency, re['ComputeLantency'], revenue, win, lose, tie)
                update_statistic(statistics_IntervalNum, re['IntervalNum'], revenue, win, lose, tie)
                update_statistic(statistics_MuUpper, re['MuUpper'], revenue, win, lose, tie)
                '''
                C_I_M = str(re['ComputeLantency']) + "_" + str(re['IntervalNum']) + "_" + str(re['InMuUpper'])
                update_statistic(statistics_C_I_M, C_I_M, revenue, win, lose, tie)

                count += 1
                total_revenue += revenue

            start += offset
    else:
        print("range:")
        print(range)
        cur.execute(sql)
        result = cur.fetchall()

        for re in result:
            revenue = re['Revenue']
            print revenue
            if revenue > 0:
                win = 1
                lose = 0
                tie = 0
                win_count += 1
                win_revenue += revenue
            else:
                if revenue < 0:
                    win = 0
                    lose = 1
                    tie = 0
                    lose_count += 1
                    lose_revenue += revenue
                else:
                    win = 0
                    lose = 0
                    tie = 1
                    tie_count += 1
            '''
            update_statistic(statistics_ComputeLantency, re['ComputeLantency'], revenue, win, lose, tie)
            update_statistic(statistics_IntervalNum, re['IntervalNum'], revenue, win, lose, tie)
            update_statistic(statistics_MuUpper, re['InMuUpper'], revenue, win, lose, tie)
            '''
            C_I_M = str(re['ComputeLantency']) + "_" + str(re['IntervalNum']) + "_" + str(re['InMuUpper'])
            update_statistic(statistics_C_I_M, C_I_M, revenue, win, lose, tie)

            count += 1
            total_revenue += revenue



    print("finished statistic and start to write db")

    average_revenue = total_revenue / count
    win_percent = float(win_count) / count
    lose_percent = float(lose_count) / count
    tie_percent = float(tie_count) / count
    '''
    write_stats_db(cur,conn,"","",time,range,total_revenue,average_revenue,count,win_percent,lose_percent,tie_percent)
    write_dict_db(statistics_ComputeLantency,cur,conn,"ComputeLantency",time,range)
    write_dict_db(statistics_IntervalNum, cur, conn, "IntervalNum", time, range)
    write_dict_db(statistics_MuUpper, cur, conn, "MuUpper", time, range)
    '''
    write_dict_db(statistics_C_I_M, cur, conn, "C_I_M", time, range)

    print("total revenue: "+ str(total_revenue))
    print("average revenue: " + str(average_revenue))
    print("record num: " + str(count))
    print("win percent: " + str(win_percent))
    print("lose percent: " + str(lose_percent))
    print("tie percent: " + str(tie_percent))

    print("statistics_ComputeLantency")
    for key in statistics_ComputeLantency:
        print(statistics_ComputeLantency[key])

    print("statistics_IntervalNum")
    for key in statistics_IntervalNum:
        print(statistics_IntervalNum[key])

    print("statistics_MuUpper")
    for key in statistics_MuUpper:
        print(statistics_MuUpper[key])


#statistic for all data
def statistic_all(cur,conn):
    sql = "select ComputeLantency,IntervalNum, MuUpper,Revenue from revenue"
    print("statistic_all:")
    sql2 = "select count(*) from revenue"
    cur.execute(sql2)
    record_num = cur.fetchall()[0]['count(*)']
    statistic(cur,conn,sql,record_num,"2013","0")  #range = 0 means not top
    print("-----------------------------------------------------------------------------------")

#statistic for one month data
def statistic_month(cur, conn, month, tablename):
    sql = "select ComputeLantency,IntervalNum, InMuUpper,Revenue from " + tablename + " where Times like '" + month + "%'"
    sql2 = "select count(*) from " + tablename + " where Times like '" + month + "%'"
    cur.execute(sql2)
    record_num = cur.fetchall()[0]['count(*)']
    print("statistic_month ")
    print(month)
    statistic(cur,conn,sql,record_num, month,"1")
    print("-----------------------------------------------------------------------------------")

#statistic for top revenue in all data
def statistic_top_all(cur,conn, top):
    sql = "select ComputeLantency,IntervalNum, MuUpper,Revenue from revenue order by Revenue DESC limit " + str(top)

    print("statistic_all_top ")
    print(top)
    statistic(cur,conn,sql,0,"2013","1")
    print("-----------------------------------------------------------------------------------")

#statistic for top revenue in one month
def statistic_top_month(cur, conn, month, top):
    sql = "select ComputeLantency,IntervalNum, MuUpper,Revenue from revenue where Times like '" + month + "%'" \
         "order by Revenue DESC limit " + str(top)

    print("statistic_month_top ")
    print(month)
    print(top)
    statistic(cur,conn,sql,0,month,"1")
    print("-----------------------------------------------------------------------------------")

def is_trade_day(cur,day,tablename):
    sql = "select count(*) from " + tablename + " where Times like '" + day + "%'"
    cur.execute(sql)
    result = cur.fetchall()[0]['count(*)']
    if int(result) > 0:
        return True
    else:
        return False


def statistic_day(cur,conn,tablename,top,month):
    day_list = []
    for i in range(1,10):
        day_list.append(month + "0" + str(i))
    for i in range(10,32):
        day_list.append(month+str(i))
    print(day_list)

    for day in day_list:
        if is_trade_day(cur,day,tablename):
            statistic_month(cur,conn,day)



def find_candidate_day(cur,sql,candidate):
    cur.execute(sql)
    result = cur.fetchall()

    for re in result:
        if re['Times'] not in candidate:
            candidate.setdefault(re['Times'], re['C_I_M'])

def find_day(cur,tablename,offset):
    sql_good = "select * from "+ tablename +" where length(Times)>6 and isTop=0 order by average_revenue DESC limit "  + offset
    sql_bad = "select * from "+ tablename +" where length(Times)>6 and isTop=0 order by average_revenue limit " + offset
    sql_position = "select count(*) from "+ tablename +" where length(Times)>6 and isTop=0"
    cur.execute(sql_position)
    average_position = cur.fetchall()[0]['count(*)']
    position = int(int(average_position) - int(offset)/2)
    sql_average = "select * from " + tablename +" where length(Times)>6 and isTop=0 order by average_revenue limit " + str(position)+ ", " + offset

    good_day = {}
    bad_day = {}
    average_day = {}
    find_candidate_day(cur, sql_good, good_day)
    find_candidate_day(cur,sql_bad, bad_day)
    find_candidate_day(cur,sql_average, average_day)

    candidate_day = {}
    C_I_M = []
    for day in good_day:
        if day in bad_day and day in average_day:
            C_I_M.append(good_day[day])
            C_I_M.append(bad_day[day])
            C_I_M.append(average_day[day])
            candidate_day.setdefault(day,C_I_M)

    return candidate_day










#clear_table
def clear_table(cur,conn,tablename):
    sql = "delete from " + tablename
    cur.execute(sql)
    conn.commit()




conn1 = connect_db('localhost','stockresult','root','0910@mysql')
cur1 = conn1.cursor()

'''
sql = "SELECT ComputeLantency, IntervalNum, MuUpper, Day,avg(revenue),sum(revenue),count(*)  FROM revenue20130625 " \
      "group by ComputeLantency, IntervalNum, MuUpper, Day"
cur1.execute(sql)
result = cur1.fetchall()
stats = {}
for re in result:
    key = (re['ComputeLantency'],re['IntervalNum'],re['MuUpper'])
    print(key)
    print(stats)
    if key in stats:
        print("adhf")
        stats[key][3] += 1
        stats[key][0] += re['avg(revenue)']
        stats[key][1] += re['sum(revenue)']
        stats[key][2] += re['count(*)']
    else:
        value = [re['avg(revenue)'],re['sum(revenue)'],re['count(*)'],1]
        stats.setdefault(key,value)

for key in stats:
    stats[key][0] /= stats[key][3]
    stats[key][1] /= stats[key][3]
    stats[key][2] /= stats[key][3]

    print(str(key[0])+","+ str(key[1]) + "," + str(key[2]) + "," , end="")
    print(str(stats[key][1]) + ","+ str(stats[key][0]) + "," + str(stats[key][2]))
'''


#drop_table(cur1, conn1, "revenue20170426stop")
#tablename ="revenue20170901"
'''
features = "Day varchar(50)," \
           "Times varchar(50), " \
           "LastPrice double, " \
           "isOpen int(11), " \
           "isLong int(11), " \
           "ComputeLantency int(11), " \
           "IntervalNum int(11), " \
           "inMuUpper double, " \
           "outMuUpper double," \
           "StopDown int(11), " \
           "StopUp int(11), " \
           "Revenue double"

features = "Day varchar(50)," \
           "Times varchar(50), " \
           "LastPrice double, " \
           "isOpen int(11), " \
           "isLong int(11), " \
           "ComputeLantency int(11), " \
           "IntervalNum int(11), " \
           "InMuUpper double," \
           "lnLastPriceThreshold double,"\
           "Revenue double"


create_table(cur1,conn1,tablename, features)
'''

month_list = {}
from_tables = ["tradeinfos20170921"]
to_tables = ["revenue20170921"]
tables = zip(from_tables, to_tables)
db = DB('localhost', 'stockresult','root','0910@mysql')
add_cols = {'Revenue': "double"}

for (from_table, to_table) in tables:
    print from_table, to_table
    db.create_table_copy(from_table, to_table, add_cols=add_cols)
    write_revenue_db(cur1, conn1, month_list, from_table, to_table)
    for month in month_list:
        print(month)



'''
features = "C_I_M varchar(50)," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           " PRIMARY KEY (`C_I_M`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_c_i_m",features)
statistic_month(cur1, conn1, "201306", "revenue20170901")



features = "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           "PRIMARY KEY (`Times`,`isTop`)"
create_table(cur1,conn1,"stats",features)
features = "ComputeLantency int(11)," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           "PRIMARY KEY (`ComputeLantency`, `Times`,`isTop`)"

create_table(cur1,conn1,"stats_computelantency",features)
features = "IntervalNum int(11)," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           "PRIMARY KEY (`IntervalNum`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_intervalnum",features)
features = "MuUpper double," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           "PRIMARY KEY (`MuUpper`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_muupper",features)
features = "C_I_M varchar(50)," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           " PRIMARY KEY (`C_I_M`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_c_i_m",features)
'''

'''
clear_table(cur1,conn1,"stats")
clear_table(cur1,conn1,"stats_computelantency")
clear_table(cur1,conn1,"stats_intervalnum")
clear_table(cur1,conn1,"stats_muupper")
clear_table(cur1,conn1,"stats_c_i_m")

print("start to statistic:")

statistic_all(cur1,conn1)
statistic_top_all(cur1, conn1,1000)

#month_list = ["201303","201304","201305","201306","201309","201310","201311","201312","201405","201411"]
month_list = ["201303","201304","201305","201306","201307","201308","201309","201310","201311","201312","201405","201411"]
for month in month_list:
    statistic_month(cur1,conn1, month)
    statistic_top_month(cur1,conn1, month, 1000)

features = "C_I_M varchar(50)," \
           "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \
           " PRIMARY KEY (`C_I_M`, `Times`,`isTop`)"
create_table(cur1,conn1,"day_stats_c_i_m",features)
'''
#statistic_day(cur1,conn1)

'''
#month_list = ["201303","201304","201305","201306","201307","201308","201309","201310","201311","201312"]
for month in month_list:
    statistic_day(cur1,conn1,"revenue","0",month)
'''
'''


#candidate_day = find_day(cur1,"stats_c_i_m","1000")
'''
'''
feature_common = "Times varchar(50), " \
           "isTop int(11), " \
           "total_revenue double," \
           "average_revenue double," \
           "count int(11), " \
           "win_percent double, " \
           "lose_percent double," \
           "tie_percent double," \

features = "Mu double," + feature_common + " PRIMARY KEY (`Mu`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_Mu",features)
features = "Sigma double," + feature_common + " PRIMARY KEY (`Sigma`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_Sigma",features)
features = "LossingStopUp double," + feature_common + " PRIMARY KEY (`LossingStopUp`, `Times`,`isTop`)"
create_table(cur1,conn1,"stats_LossingStopUp",features)
clear_table(cur1,conn1,"stats_Mu")
clear_table(cur1,conn1,"stats_Sigma")
clear_table(cur1,conn1,"stats_LossingStopUp")
day_list = ['20131010', '20131011', '20131016', '20131021', '20131023','20131025','20131029']
for day in day_list:
    sql = "select Mu,Sigma,LossingStopUp, Revenue from InnerValue_revenue where Times like '" + day + "%'"
    sql2 = "select count(*) from revenue where Times like '" + day + "%'"
    cur1.execute(sql2)
    record_num = cur1.fetchall()[0]['count(*)']
    cur1.execute(sql)
    result = cur1.fetchall()
    statistics_Mu = {}
    statistics_Sigma = {}
    statistics_LossingStopUp = {}

    win_count = 0
    lose_count = 0
    tie_count = 0
    count = 0
    total_revenue = 0
    win_revenue = 0
    lose_revenue = 0

    for re in result:
        revenue = re['Revenue']
        if revenue > 0:
            win = 1
            lose = 0
            tie = 0
            win_count += 1
            win_revenue += revenue
        else:
            if revenue < 0:
                win = 0
                lose = 1
                tie = 0
                lose_count += 1
                lose_revenue += revenue
            else:
                win = 0
                lose = 0
                tie = 1
                tie_count += 1
        count += 1
        total_revenue += revenue
        update_statistic(statistics_Mu, re['Mu'], revenue, win, lose, tie)
        update_statistic(statistics_Sigma, re['Sigma'], revenue, win, lose, tie)
        update_statistic(statistics_LossingStopUp, re['LossingStopUp'], revenue, win, lose, tie)

    print("finished statistic and start to write db")

    average_revenue = total_revenue / count
    win_percent = win_count / count
    lose_percent = lose_count / count
    tie_percent = tie_count / count


    write_dict_db(statistics_Mu, cur1, conn1, "Mu", day, 0)
    write_dict_db(statistics_Sigma, cur1, conn1, "Sigma", day, 0)
    write_dict_db(statistics_LossingStopUp, cur1, conn1, "LossingStopUp",day, 0)
'''

close_db(cur1, conn1)