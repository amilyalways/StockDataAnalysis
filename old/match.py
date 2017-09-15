from StockDataAnalysis.Utility.DB import DB
import time
import pandas as pd
db = DB('localhost', 'stockresult','root','0910mysql@')

'''
sql = "select * from revenue20170426 where Revenue>=30"
db.cur.execute(sql)
result = db.cur.fetchall()
for re in result:
    sql = "select ID from tradeinfos20170426 where Times='" + re['Times'] + "' and isOpen=0 and "
    sql += "ComputeLantency=" + str(re['ComputeLantency']) + " and IntervalNum=" + str(re['IntervalNum'])
    sql += " and MuUpper=" + str(re['outMuUpper'])
    #print(sql)
    db.cur.execute(sql)
    result1 = db.cur.fetchall()

    id = result1[0]['ID'] - 1

    sql2 = "select * from tradeinfos20170426 where ID=" + str(id)
    #print(sql2)
    db.cur.execute(sql2)
    result2 = db.cur.fetchall()

    re2=result2[0]
    sql3 = "select * from tradeinfos20170426stop where Times='" + re2['Times'] + "' and isOpen=1 and "
    sql3 += "ComputeLantency=" + str(re2['ComputeLantency']) + " and IntervalNum=" + str(re2['IntervalNum'])
    sql3 += " and MuUpper=" + str(re2['MuUpper'])
    #print(sql3)
    db.cur.execute(sql3)
    result3 = db.cur.fetchall()
    if len(result3)>0:
        print("Match")
        print(re2)
        print(result3[0])

    print("--------------------------------------")
'''

'''
sql = "select ComputeLantency,IntervalNum,inMuUpper,outMuUpper,count(*) from revenue20170428 "
sql += "group by ComputeLantency,IntervalNum,inMuUpper,outMuUpper"
print(sql)

db.cur.execute(sql)
result = db.cur.fetchall()

stats = {}
for re in result:
    key = (re['ComputeLantency'],re['IntervalNum'],re['inMuUpper'],re['outMuUpper'])
    value = {'Num':re['count(*)'], 't_Num':0}
    stats.setdefault(key,value)

sql = "select ComputeLantency,IntervalNum,inMuUpper,outMuUpper,count(*) from revenue20170428 "
sql += "where tag=1 or tag=-1 group by ComputeLantency,IntervalNum,inMuUpper,outMuUpper"
print(sql)

db.cur.execute(sql)
result = db.cur.fetchall()
for re in result:
    key = (re['ComputeLantency'], re['IntervalNum'], re['inMuUpper'], re['outMuUpper'])
    if key in stats:
        stats[key]['t_Num'] = re['count(*)']

for s in stats:
    print(str(s[0]) + "," + str(s[1]) + "," +str(s[2]) + "," +str(s[3]) + "," ,end="")
    print(str(stats[s]['Num']) +  "," + str(stats[s]['t_Num']))
'''
def time_to_long(Times):
    L = time.mktime(time.strptime(Times[:17],'%Y%m%d-%H:%M:%S'))
    L = L + int(Times[18:])*0.001
    return L

'''
tablename = "tradeinfos20170428"
sql = "select ID from "+ tablename+ " where tag=1 or tag=-1"
db.cur.execute(sql)
result = db.cur.fetchall()
id_list = []
for re in result:
    arr_i = [-1,0,1,2]
    for i in arr_i:
        id = re['ID'] + i
        if id not in id_list and id<347177:
            id_list.append(id)

output = open("C:\\Users\\songxue\\Desktop\\aa.txt",'w+')

for id in id_list:
    sql = "select * from "+ tablename+ " where ID=" + str(id)

    db.cur.execute(sql)
    result1 = db.cur.fetchall()
    for re in result1:

        sql4 = "insert into tradeinfos_stop" + " (ID,Times,LastPrice,isOpen,isLong,ComputeLantency," \
                                                "IntervalNum, MuUpper,MuLower,tag) values( "
        sql4 += str(re['ID']) + ",'" + str(re['Times']) + "', " + str(re['LastPrice']) + ", " + str(
            re['isOpen'])
        sql4 += ", " + str(re['isLong']) + ", " + str(re['ComputeLantency']) + ", " + str(
            re['IntervalNum']) + ", "
        sql4 += str(re['MuUpper']) + ", " + str(re['MuLower']) + ", " + str(re['tag']) + ") "
        #print(sql4)
        db.cur.execute(sql4)
        db.conn.commit()
        # LTimes = time_to_long(re1['Times'])


        line = str(re1['ID']) + ","  + str(re1['Times'][:8]) + ","+ str(re1['Times']) + "," + str(LTimes) + "," + str(re1['LastPrice']) + "," + str(re1['isOpen']) + ","
        line += str(re1['isLong']) + "," + str(re1['ComputeLantency']) + "," + str(re1['IntervalNum']) + "," + str(re1['MuUpper']) + ","
        line += str(re1['MuLower']) + "," + str(re1['tag']) + "\n"
        output.write(line)

'''
'''
sql = "select ID,Times from tradeinfos_stop where tag=1 or tag=-1"
db.cur.execute(sql)
result =db.cur.fetchall()

time1 = ""
time2 = ""
for re in result:
    id = re['ID']
    if id < 347176:
        id1 = id +1
        time1 = re['Times']

        sql2 = "select Times from tradeinfos_stop where ID=" + str(id1)
        db.cur.execute(sql2)
        result2 = db.cur.fetchall()
        time2 = result2[0]['Times']

        I_time = time_to_long(time2) - time_to_long(time1)
        id2 = id + 2
        sql3 = "update revenue_stop set I_time=" + str(I_time) + " where ID=" + str(id2)

        print(sql3)
        db.cur.execute(sql3)
        db.conn.commit()
'''
'''
t_list = [10,30,60,90,120,300,600,25200]
for t in t_list:
    sql = "SELECT count(*) FROM revenue_stop where I_time>0 and I_time<" + str(t) + " and tag=-1"
    db.cur.execute(sql)
    result = db.cur.fetchall()
    #print(t)
    print(result[0]['count(*)'])
'''

'''
sql = "select Times from data201306"
db.cur.execute(sql)
result = db.cur.fetchall()
for re in result:
    L_time = time_to_long(re['Times'])
    sql = "update data201306 set L_time=" + str(L_time) + " where Times='" + re['Times'] +"'"
    db.cur.execute(sql)
    db.conn.commit()
'''

def match(file1,file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    L = len(df1["Times"])
    print(L)
    j = 1
    L2 = len(df2["Times"])
    for i in range(1,L):
        if isMatch(df1["Times"][i],df2["Times"][j]):
            df1['0.0001Mu'][i] = df2['0.0001Mu'][j]
            df1['0.0002Mu'][i] = df2['0.0002Mu'][j]
            df1['0.0003Mu'][i] = df2['0.0003Mu'][j]
            df1['0.0004Mu'][i] = df2['0.0004Mu'][j]
            df1['0.0005Mu'][i] = df2['0.0005Mu'][j]
            df1['0.001Mu'][i] = df2['0.0005Mu'][j]
            print(j)
            j += 1
            if j >= L2:
                break

    df1.to_csv(file1)

def isMatch(a,b):
    if a == b:
        return True
    else:
        return False

def db_csv(table,path,ComputeLantency,IntervalNum,arr_LnLastPrice):
    sql = "select Times,Mu from " + table + " where ComputeLantency=" + str(ComputeLantency) + " and "
    sql += " IntervalNum=" + str(IntervalNum)
    file1 = path + "Mu_" + str(ComputeLantency) + "_" + str(IntervalNum) + ".csv"
    print(file1)
    df_csv = pd.read_csv(file1)

    for LnLastPrice in arr_LnLastPrice:
        sql_t = sql + " and lnLastPriceThreshold=" + str(LnLastPrice)
        df_db = pd.read_sql(sql_t,db.conn)
        df_csv["Times"] = df_db["Times"]
        i = str(LnLastPrice)+ "Mu"
        df_csv[i] = df_db["Mu"]
    df_csv.to_csv(file1)






path1 = "C:\\Users\\songxue\\Desktop\\statsReturn20170612\\"
path2 = "C:\\Users\\songxue\\Desktop\\"
arr_ComputeLantency = [5,10,20]
arr_IntervalNum = [10,20]
for c in arr_ComputeLantency:
    for i in arr_IntervalNum:
        file1 = path1 + "statsReturn" + str(c) + "_" + str(i) + ".csv"
        file2 = path2 + "Mu_" + str(c) + "_" + str(i) + ".csv"
        match(file1,file2)

'''
path = "C:\\Users\\songxue\\Desktop\\"
arr_LnLastPrice = [0.0001,0.0002,0.0003,0.0004,0.0005,0.001]
arr_ComputeLantency = [5,10,15,20]
arr_IntervalNum = [10,20]
for c in arr_ComputeLantency:
    for i in arr_IntervalNum:
        db_csv("20170616mu",path,c,i,arr_LnLastPrice)
'''










