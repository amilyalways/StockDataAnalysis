from numpy import mean
from numpy import std
import math
import matplotlib.pyplot as plt
from StockDataAnalysis.Utility.DB import DB
import os
import csv
import time

def time_to_long(Times):
    L = time.mktime(time.strptime(Times[:17],'%Y%m%d-%H:%M:%S'))
    L = L + int(Times[18:])*0.001
    return L

def long_to_time(L):
    times = time.localtime(L)
    return time.strftime('%Y%m%d-%H:%M:%S',times)

def time_to_x(times,start):
    t = time_to_long(times)
    return t-start

def isSample(times,sample):
    t1 = times[:12] + "00:00 0"
    L1 = time_to_long(t1)
    L2 = time_to_long(times)
    L = int(L2 - L1)
    if L % sample == 0:
        return True
    return False

def get_null_sample_num(times,sample):
    t1 = times[:12] + "00:00 0"
    L1 = time_to_long(t1)
    L2 = time_to_long(times)
    L = L2 - L1
    return int(L/sample)




def get_hour_time_series(db,tablename, sample):

    sql = "select * from " + tablename
    db.cur.execute(sql)
    result = db.cur.fetchall()
    hour_list = {}
    print(hour_list)
    start_list = {}

    before_times = result[0]['Times'][:17]
    for re in result:
        key = re['Times'][:11]
        if before_times != re['Times'][:17] and isSample(re['Times'], sample):
            if key in hour_list:
                hour_list[key].append(re['LastPrice'])
            else:
                start_list.setdefault(key, get_null_sample_num(re['Times'], sample))
                value = [re['LastPrice']]
                hour_list.setdefault(key, value)
        before_times = re['Times'][:17]

    print("start to write to file")
    path = "C:\\Users\\songxue\\Desktop\\"
    filename = "Normal_hour_TimeSeries2.txt"

    f = open(path + filename, 'a')
    data = ""
    for hour in hour_list:
        #print(hour_list[hour])
        mean1 = mean(hour_list[hour])
        std1 = std(hour_list[hour])
        data += hour
        print(start_list)
        print(hour_list[hour])

        for i in range(start_list[hour]):
            data += ", "
        for i in hour_list[hour]:
            data += "," + str((i - mean1) / std1)

        data += "\n"

    f.writelines(data)








'''
f = open("C:\\Users\\songxue\\Desktop\\m.txt",'r')

lines = f.readlines()
arr_y = []
for line in lines:
    line = line[:-2]
    temp = line.split("\t")
    y = []
    print(temp)

    for i in temp[2:]:
        if i == "":
            break
        y.append(float(i))
    arr_y.append(y)



for y in arr_y:
    plt.plot(y)
plt.show()

'''


def get_cluster(cluster_list,path):
    f = open(path, 'r')
    lines = f.readlines()

    for line in lines:
        line = line[:-1]
        temp = line.split("\t")
        if temp[1] in cluster_list:
            cluster_list[temp[1]].append(temp[0])
        else:
            cluster_list.setdefault(temp[1],[temp[0]])

def stats_cluster(db,cluster,arr_hour,tablename):
    for hour in arr_hour:
        sql = "insert into " + tablename + " select Times, @cluster:='" + cluster + "', ComputeLantency,IntervalNum,MuUpper, AVG(Revenue) " \
          "from stockresult.revenue_1 where Times like '" + hour + "%'group by ComputeLantency,IntervalNum,MuUpper " \
          "order by AVG(Revenue) DESC limit 100"
        db.cur.execute(sql)
        db.conn.commit()




def plot_cluster_trend(db,cluster, arr_hour):
    plt.figure(figsize=(60,35))
    for hour in arr_hour:
        tablename = "data" + hour[:6]
        sql = "select * from " + tablename + " where Times like '" + hour + "%'"
        print(sql)
        db.cur.execute(sql)
        result = db.cur.fetchall()
        arr_y = []
        arr_x = []
        count = 0
        start_time = hour + ":00:00 0"
        print(start_time)
        start = time_to_x(start_time, 0)
        end = long_to_time(start+3600)
        print(end)


        for re in result:
            if count % 20 == 0:
                arr_y.append(re['LastPrice'])
                arr_x.append(time_to_x(re['Times'], start))
        nor_y = []
        mean_y = mean(arr_y)
        std_y = std(arr_y)
        for y in arr_y:
            nor_y.append(float((y-mean_y)/std_y))

        plt.plot(arr_x,nor_y,linewidth=1)

    plt.ylim(-5,5)
    plt.xlim(0, 3600)

    plt.ylabel("Normalization LastPrice")
    plt.xlabel("Times")
    x_ticks = []
    j = 0
    for i in range(3600):
        if j % 10 == 0:
            x_ticks.append(long_to_time(j+start)[12:])
        else:
            x_ticks.append("")
        j += 1


    plt.xticks(range(len(x_ticks)),x_ticks,rotation=90)
    path = "C:\\Users\\songxue\\Desktop\\cluster_trend_normal\\"
    figname = cluster
    plt.savefig(path+figname)
    plt.close()









db = DB('localhost', 'stockresult', 'root', '0910mysql@')

'''
month_list = ["201303","201304","201305","201306","201307","201308","201309","201310","201311","201312","201405","201411"]
for month in month_list:
    tablename = "data" + month
    print(tablename)
    get_hour_time_series(db,tablename,10)
'''
'''
cluster_list = {}
get_cluster(cluster_list)
for cluster in cluster_list:
    print(cluster,end="")
    print("   ",end="")
    print(len(cluster_list[cluster]) ,end="")
    print("   ", end="")
    print(cluster_list[cluster])
    stats_cluster(db,cluster,cluster_list[cluster],"stats_feature_by_hour")
'''


sql = "select cluster,ComputeLantency,IntervalNum,MuUpper, AVG(averageRevenue) from stats_feature_by_hour" \
      " group by cluster,ComputeLantency,IntervalNum,MuUpper" \
      " order by AVG(averageRevenue) desc"
db.cur.execute(sql)
result = db.cur.fetchall()
cluster_revenue = {}
for re in result:
    if re['cluster'] in cluster_revenue:
        if len(cluster_revenue[re['cluster']]) < 10:
            cluster_revenue[re['cluster']].append(re)
    else:
        cluster_revenue.setdefault(re['cluster'], [re])

for cluster in cluster_revenue:
    print("-----------"+ cluster + "-----------------------")
    for re in cluster_revenue[cluster]:
        print(re)
    print("*****************************************************")


''''
cluster_list = {}
get_cluster(cluster_list)
for cluster in cluster_list:
    plot_cluster_trend(db,cluster,cluster_list[cluster])
'''

def update_cluster_day(db,tablename):
    cluster_list = {}
    get_cluster(cluster_list)
    for cluster in cluster_list:
        for day in cluster_list[cluster]:
            sql = "update " + tablename + " set cluster='" + cluster + "' where times like '" + day + "%'"
            print(sql)
            db.cur.execute(sql)
            db.conn.commit()


#update_cluster_day(db,"stats_feature_by_hour")






