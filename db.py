#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql

def ComputeRevenue(price1,price2,isLong):
    if isLong == 0:
        t = -1
    else:
        t = 1
    return (price2-price1)*t

def UpdateStatistic(dict,key, revenue,):
    value = {'count':1,  'totalRevenue': revenue, 'averRevenue': revenue }
    if key in dict:
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

# connect to db
config = {
          'host':'192.168.1.134',
          'port':3306,
          'user':'root',
          'password':'not(Null)',
          'db':'stockresult',
          'charset':'utf8mb4',
          'cursorclass':pymysql.cursors.DictCursor,
          }
conn = pymysql.connect(**config)

# select data from db
cur = conn.cursor()
'''
sql = "select count(*) from tradeinfoswithmu limit 1000"
cur.execute(sql)
result = cur.fetchall()
print(result)
'''
sql = "select LastPrice, isLong, IntervalNum, MuUpper, ComputeLantency from tradeinfoswithmu"
cur.execute(sql)
result = cur.fetchall()

BeforePrice = result[0]['LastPrice']

totalRevenue = 0
count = 0

statistics_IntervalNum = {}
statistics_MuUpper = {}
statistics_MuLower = {}

config1 = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'0910mysql@',
          'db':'stock',
          'charset':'utf8mb4',
          'cursorclass':pymysql.cursors.DictCursor,
          }
conn1 = pymysql.connect(**config1)
cur1 = conn1.cursor()

for re in result[1:]:
    revenue = ComputeRevenue(BeforePrice,re['LastPrice'],re['isLong'])
    totalRevenue += revenue
    count += 1
    re.setdefault('Revenue',revenue)
    sql1 = "insert into tb1 (Revenue, intervalNum, MuUpper, ComputeLantency, id) values(" + str(revenue) + ", " + str(re['IntervalNum']) + ", " + str(re['MuUpper']) + ", " + str(re['ComputeLantency']) + ", " + str(count) + ") "
    cur1.execute(sql1)

    #print(re)
    BeforePrice = re['LastPrice']

'''sql3 = "delete from tb1"
cur1.execute(sql3)


sql2 = "select * from tb1 "
cur1.execute(sql2)
result1 = cur1.fetchall()
print(result1)
'''

sql4 = "select * from tb1 order by revenue DESC limit 1000"
cur1.execute(sql4)
result1 = cur1.fetchall()

for re1 in result1:
    print(re1)
    UpdateStatistic(statistics_IntervalNum, re1['intervalNum'], re1['Revenue'])
    UpdateStatistic(statistics_MuUpper, re1['MuUpper'], re1['Revenue'])
    UpdateStatistic(statistics_MuLower, re1['ComputeLantency'], re1['Revenue'])








'''
    UpdateStatistic(statistics_IntervalNum, re['IntervalNum'], revenue)
    UpdateStatistic(statistics_MuUpper, re['MuUpper'], revenue)
    UpdateStatistic(statistics_MuLower, re['MuLower'], revenue)
'''


print("statistics_IntervalNum: ")
print(statistics_IntervalNum)
printResult(statistics_IntervalNum)
print('-------------------------------------------------------')
print("statistics_MuLower: ")
print(statistics_MuLower)
printResult(statistics_MuLower)
print('-------------------------------------------------------')
print("statistics_MuUpper: ")
print(statistics_MuUpper)
printResult(statistics_MuUpper)
print('-------------------------------------------------------')

print("totalRevenue: ")
print(totalRevenue)
averageRevenue = totalRevenue/count
print("averageRevenue: ")
print(averageRevenue)


cur.close()
conn.close()





