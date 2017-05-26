from StockDataAnalysis.Utility.DB import DB
from numpy import max
from numpy import min
import time
import pandas as pd

class Stats:

    start = 0
    offset = 100000

    def __init__(self):
        self.db = DB('localhost', 'stockresult','root','0910mysql@')

    def ComputeRevenue(self, price1, price2, isLong):
        if isLong == 0:
            t = -1
        else:
            t = 1
        return (price2 - price1) * t

    def update_stats(self, dict, key, re):
        revenue = 'Revenue'
        win = 0
        lose = 0
        tie = 0
        if re[revenue] > 0:
            win = 1
        elif re[revenue] < 0:
            lose = 1
        else:
            tie = 1
        value = {'count': 1, 'win': win, 'lose': lose, 'tie': tie, 'totalRevenue': re[revenue], 'averRevenue': re[revenue]}
        if key in dict:
            dict[key]['win'] += win
            dict[key]['lose'] += lose
            dict[key]['tie'] += tie
            dict[key]['count'] += 1
            dict[key]['totalRevenue'] += re[revenue]
            dict[key]['averRevenue'] = dict[key]['totalRevenue'] / dict[key]['count']
        else:
            dict.setdefault(key, value)
        return

    def stats_revenue(self, from_table, arr_target, **condition_list):
        condition = ""
        multi_condition = False
        if 'Times' in condition_list:
            condition += "Times like '" + str(condition_list['Times']) + "%'"
            multi_condition = True
        '''
        if 'feature_list' in condition_list:
            if multi_condition:
                condition += " and "
            for feature in feature_list:
                condition += str(feature) + "=" + str(condition_list['feature_list'][feature]) + " and "
            condition = condition[:-4]
        print(condition)
        '''
        if len(condition) > 0:
            con = {'condition': condition}
        else:
            con = {}

        record_num = self.db.select(from_table, "count(*)", **con)[0]["count(*)"]
        print(record_num)
        isTop = False
        stats_list = {}
        for arr_feature in arr_target:
            stats_list.setdefault(arr_feature)

        while self.start < record_num and not isTop:
            if 'range' in condition_list:
                con.setdefault('limit', condition_list['range'])
                isTop = True
            else:
                con.setdefault('limit', {'start': self.start, 'offset': self.offset})

            result = self.db.select(from_table, "*", **con)

            for re in result:
                for arr_feature in stats_list:
                    key = []
                    for feature in arr_feature:
                        key.append(re[feature])

                    self.update_stats(stats_list[arr_feature], key, re)

            self.start = self.start + self.offset

        return stats_list


    def stats_day_mu(self, tablename, day, time_range):
        result = self.db.select(tablename,"Mu", condition= "times like '" + day +"%'", limit={'start':0,'offset':time_range})
        print(result)
        arr_mu = []
        pos = 0
        for re in result:
            if re['Mu'] != 0:
                break
            pos += 1
        if pos == len(result):
            return -1

        for re in result[pos:]:
            arr_mu.append(re['Mu'])
        min_mu = min(arr_mu)
        max_mu = max(arr_mu)
        print(min_mu)
        print(max_mu)
        result = self.db.select(tablename,"count(*)",condition= "times like '"+ day +"%'" )
        all_num = result[0]["count(*)"]
        condition = "times like '"+ day + "%' and Mu>=" + str(min_mu) +" and Mu<=" + str(max_mu)
        result = self.db.select(tablename,"count(*)",condition=condition)
        part_num = result[0]["count(*)"]
        if min_mu <= 0 and max_mu >= 0 :
            all_num -= pos - 1
            part_num -= pos - 1


        percent = int(part_num)/int(all_num)

        return percent

    def stats_mu(self, tablename, time_range):
        result = self.db.select(tablename, "Times")
        day_list = {}
        for re in result:
            key = re['Times'][:8]
            if key not in day_list:
                day_list.setdefault(key)
        percent_list = {}
        sum_percent = 0
        count = 0
        print(day_list)
        for day in day_list:
            percent = self.stats_day_mu(tablename,day,time_range)
            percent_list.setdefault(day,percent)
            if percent != -1:
                sum_percent += percent
                count += 1
        average_percent = sum_percent/count
        print("average percent," + str(average_percent) )
        print("day num," + str(count))

        for day in percent_list:
            print(day, end="")
            print("," + str(percent_list[day]))

    def stats_distince_mu(self, tablename, time_range):
        '''
        result = self.db.select(tablename, "Times")
        day_list = {}
        for re in result:
            key = re['Times'][:8]
            if key not in day_list:
                day_list.setdefault(key)
        '''
        f = open("C:\\Users\\songxue\\Desktop\\badDay.txt", 'r')
        lines = f.readlines()
        day_list = {}
        for line in lines:
            day_list.setdefault(line[:-1])

        part_mu_num_list = {}
        all_mu_num_list = {}

        for day in day_list:
            count = 0
            mu_list = {}
            result = self.db.select(tablename, "Mu", condition="times like '" + day + "%'")
            for re in result[0:time_range]:
                if re['Mu'] not in mu_list:
                    mu_list.setdefault(re['Mu'])
                    count += 1
            part_mu_num_list.setdefault(day,count)
            for re in result[time_range:]:
                if re['Mu'] not in mu_list:
                    mu_list.setdefault(re['Mu'])
                    count += 1
            all_mu_num_list.setdefault(day, count)



        for day in part_mu_num_list:
            print(day + "," , end='')
            print(str(part_mu_num_list[day]) + "," ,end='')
            print(str(all_mu_num_list[day]) + ",", end='')
            percent = int(part_mu_num_list[day]) / int(all_mu_num_list[day])
            print(str(percent))

    def stats_vary_mu(self, tablename, time_range):
        result = self.db.select(tablename, "Times")
        day_list = {}
        for re in result:
            key = re['Times'][:8]
            if key not in day_list:
                day_list.setdefault(key)
        Mu_num_list = {}
        for day in day_list:
            result = self.db.select(tablename, "Mu", condition="times like '" + day + "%'")
            length = int(len(result)/time_range)
            vary_mu = []
            for i in range(length):
                start = i*time_range
                end = start + time_range
                before_mu = result[start]
                count = 0
                for re in result[start+1:end]:
                    if re['Mu'] != before_mu:
                        count += 1
                    before_mu = re['Mu']
                vary_mu.append(count)
                if end <= len(result):
                    for re in result[end:]:
                        if re['Mu'] != before_mu:
                            count += 1
                        before_mu = re['Mu']
                    vary_mu.append(count)
                Mu_num_list.setdefault(day,vary_mu)

        for day in Mu_num_list:
            print(day, end="")
            for num in Mu_num_list[day]:
                print("," + str(num), end="")
            print(",")

    def smooth_mu(self, from_tablename, to_tablename, a):
        result = self.db.select(from_tablename, "Times")
        day_list = {}
        for re in result:
            key = re['Times'][:8]
            if key not in day_list:
                day_list.setdefault(key)

        self.db.create_table(to_tablename,"Times varchar(45), Mu double, SmoothMu double")
        for day in day_list:
            result = self.db.select(from_tablename, "Times,Mu", condition="Times like '" + day + "%'")
            before_smooth_mu = 0
            for re in result:
                smooth_mu = a*re['Mu'] + (1-a)*before_smooth_mu
                values = "'" + re['Times'] + "'," + str(re['Mu']) + "," + str(smooth_mu)
                self.db.insert(to_tablename,values)
                before_smooth_mu = smooth_mu

    def smooth_mu_order(self, from_tablename, to_tablename, a):
        result = self.db.select(from_tablename, "Times, Mu")
        day_list = {}
        before_smooth_mu = 0
        self.db.create_table(to_tablename, "Times varchar(45), Mu double, SmoothMu double")

        for re in result:
            if re['Times'][:8] not in day_list:
                day_list.setdefault(re['Times'][:8])
                before_smooth_mu = 0

            smooth_mu = a * re['Mu'] + (1 - a) * before_smooth_mu
            values = "'" + re['Times'] + "'," + str(re['Mu']) + "," + str(smooth_mu)
            self.db.insert(to_tablename, values)
            before_smooth_mu = smooth_mu

    def stats_revenue(self,tablename,arr_paras):
        paras = ""
        for para in arr_paras:
            paras += para + ","
        paras = paras[:-1]

        sql = "select "+ paras + ",count(*),avg(Revenue) from " + tablename + " group by " + paras

        self.db.cur.execute(sql)
        result = self.db.cur.fetchall()

        stats = {}
        for re in result:
            key = ""
            for para in arr_paras:
                key += str(re[para]) +"_"
            key = key[:-1]

            value = {'tradeNum':re['count(*)'], 'winNum':0, 'win_percent':0,'avg_revenue':re['avg(Revenue)'],'total_revenue':re['count(*)']*re['avg(Revenue)'],'win_avg':0,'lose_avg':0}
            if key not in stats:
                stats.setdefault(key,value)
            else:
                print("Wrong: duplicate key")
                print(key)

        sql1 = "select "+ paras + ",count(*),avg(Revenue) from " + tablename
        sql1 += " where Revenue>0 group by " + paras
        self.db.cur.execute(sql1)
        result1 = self.db.cur.fetchall()

        for re in result1:
            key = ""
            for para in arr_paras:
                key += str(re[para]) + "_"
            key = key[:-1]

            if key in stats:
               stats[key]['winNum'] = re['count(*)']
               stats[key]['win_percent'] = stats[key]['winNum']/stats[key]['tradeNum']
               stats[key]['win_avg'] = re['avg(Revenue)']
               if(stats[key]['tradeNum']-re['count(*)'] != 0):
                   stats[key]['lose_avg'] = (stats[key]['total_revenue'] - re['count(*)']*re['avg(Revenue)'])/(stats[key]['tradeNum']-re['count(*)'])

        return stats

    def time_to_long(self,Times):
        L = time.mktime(time.strptime(Times[:17], '%Y%m%d-%H:%M:%S'))
        L = L + int(Times[18:]) * 0.001
        return L

    def max_win_lose_one_trade(self,t1,t2,OpenPrice,trend_table,tag,isLong):
        L1 = self.time_to_long(t1) + 0.5
        L2 = self.time_to_long(t2)

        sql = "select * from " + str(trend_table) + " where L_time between " + str(L1) + " and " + str(L2)

        self.db.cur.execute(sql)
        result = self.db.cur.fetchall()


        D_max = {'D_Price':0, 'Times':"null"}
        for re in result:
            D_price = tag * self.ComputeRevenue(OpenPrice,re['LastPrice'],isLong)


            if D_price > 0 and D_price > D_max['D_Price']:
                D_max['Times'] = re['Times']
                D_max['D_Price'] = D_price
        D_max['D_Price'] *= tag

        return D_max


    #统计每笔交易，从交易开始跌的最大值(当这笔交易是盈利的)，从交易开始涨的最大值(当这笔交易是亏损的)
    def stats_inner_max(self, trade_table, trend_table, stats_table):
        offset = 100000
        sql1 = "select count(*) from " + str(trade_table)
        self.db.cur.execute(sql1)
        record_num = self.db.cur.fetchall()[0]['count(*)']
        start = 0
        print(record_num)

        filedlist = "OpenTimes varchar(50), " \
                    "CloseTimes varchar(50), " \
                    "DMaxTimes varchar(50), " \
                    "Revenue double, " \
                    "Dmax double, " \
                    "isLong int(11),"\
                    "ComputeLantency int(11), " \
                    "IntervalNum int(11), " \
                    "inMuUpper double, " \
                    "outMuUpper double"

        self.db.create_table(stats_table, filedlist)

        while start < record_num:
            sql2 = "select * from " + trade_table + " limit " + str(start) + ", " + str(offset)
            print(sql2)

            self.db.cur.execute(sql2)
            result = self.db.cur.fetchall()
            for re in result:
                if re['isOpen'] == 0:
                    revenue = self.ComputeRevenue(OpenPrice, re['LastPrice'], re['isLong'])
                    close_time = re['Times']
                    if revenue > 0:
                        tag = 1
                    else:
                        tag = -1
                    D_max = self.max_win_lose_one_trade(open_time,close_time,OpenPrice,trend_table,tag,re['isLong'])

                    sql3 = "insert into " + str(stats_table) + " (OpenTimes,CloseTimes,DMaxTimes,Revenue,DMax,isLong,ComputeLantency," \
                                                            "IntervalNum, inMuUpper,outMuUpper ) values( '"
                    sql3 += str(open_time) + "','" + str(close_time) + "', '" + str(D_max['Times']) + "', " + str(
                        revenue) + ", " + str(D_max['D_Price'])
                    sql3 += ", " + str(re['isLong']) + ", " + str(re['ComputeLantency']) + ", " + str(re['IntervalNum']) + ", " + str(inMuUpper) + ", " + str(re['MuUpper']) + ")"
                    #print(sql3)
                    self.db.cur.execute(sql3)
                    self.db.conn.commit()
                else:
                    open_time = re['Times']
                    OpenPrice = re['LastPrice']
                    inMuUpper = re['MuUpper']

            start = start + offset

    #统计Dmax和Revenue之间的相关关系
    def stats_Dmax_Revenue(self,stats_table):

        arr_type = ["Revenue>0", "Revenue<0"]

        for type in arr_type:
            result = self.db.select(stats_table, "distinct ComputeLantency,IntervalNum,inMuUpper,outMuUpper",condition=type)
            for re in result:

                sql = "select Revenue,Dmax from " + stats_table + " where ComputeLantency=" + str(
                    re['ComputeLantency']) + " and IntervalNum="
                sql += str(re['IntervalNum']) + " and inMuUpper=" + str(re['inMuUpper']) + " and outMuUpper=" + str(
                    re['outMuUpper'])
                sql += " and " + type
                # print(sql)
                df = pd.read_sql(sql, self.db.conn)
                corr = df.corr()

                print(str(re['ComputeLantency']) + "," + str(re['IntervalNum'])+ "," + str(re['inMuUpper']) + "," + str(
                    re['outMuUpper']) + "," + str(corr['Revenue']['Dmax']))
            print("---------------------------------------------------------------------")
            print("---------------------------------------------------------------------")

    def isNone(self, value):
        a = 2.3
        b=2

        if type(value) == type(a) or type(value) == type(b):
            return value
        else:
            return 0

    #统计绝对值大于特定Dmax下，Revenue的情况
    def stats_Dmax_Reveune_percent(self,stats_table1,stats_table2):
        arr_Dmax = [-6,-8,-10,-12,-14,-16]
        result = self.db.select(stats_table1, "distinct ComputeLantency,IntervalNum,inMuUpper,outMuUpper")

        for re in result:
            condition = " where ComputeLantency=" + str(
                re['ComputeLantency']) + " and IntervalNum="
            condition += str(re['IntervalNum']) + " and inMuUpper=" + str(re['inMuUpper']) + " and outMuUpper=" + str(
                re['outMuUpper'])
            sql = "select count(*) from " + stats_table1 + condition

            self.db.cur.execute(sql)
            total = self.db.cur.fetchall()[0]['count(*)']

            for dmax in arr_Dmax:
                abs_dmax = abs(dmax)
                sql1 = "select count(*),avg(Revenue) from " + stats_table1 + condition + " and abs(Dmax)>=" + str(abs_dmax)
                sql2 = "select count(*),avg(Revenue) from " + stats_table2 + condition + " and abs(Dmax)>=" + str(abs_dmax)

                self.db.cur.execute(sql1)
                result1 = self.db.cur.fetchall()[0]
                self.db.cur.execute(sql2)
                result2 = self.db.cur.fetchall()[0]
                total = result1['count(*)'] + result2['count(*)']

                total_avg_revenue = self.isNone(result1['avg(Revenue)']) + self.isNone(result2['avg(Revenue)'])

                sql3 = sql1 + " and Revenue>0"
                sql4 = sql2 + " and Revenue>0"

                self.db.cur.execute(sql3)
                win1 = self.db.cur.fetchall()[0]
                self.db.cur.execute(sql4)
                win2 = self.db.cur.fetchall()[0]
                win = win1['count(*)'] + win2['count(*)']

                win_avg_revenue = self.isNone(win1['avg(Revenue)']) + self.isNone(win2['avg(Revenue)'])

                sql3 = sql1 + " and Revenue<0"
                sql4 = sql2 + " and Revenue<0"

                self.db.cur.execute(sql3)
                lose1 = self.db.cur.fetchall()[0]
                self.db.cur.execute(sql4)
                lose2 = self.db.cur.fetchall()[0]
                lose = lose1['count(*)'] + lose2['count(*)']

                lose_avg_revenue = self.isNone(lose1['avg(Revenue)']) + self.isNone(lose2['avg(Revenue)'])


                output = str(re['ComputeLantency']) + "," + str(re['IntervalNum']) + "," + str(re['inMuUpper']) + "," + str(re['outMuUpper']) + ","
                output += str(dmax) + "," + str(total) + "," + str(win) + "," + str(lose)
                output += "," + str(total_avg_revenue) + "," + str(win_avg_revenue) + "," + str(lose_avg_revenue)
                print(output)

















    def write_stats_db(self, stats_list, fieldlist, Times, isTop, version):
        common_column = ['total_revenue', 'average_revenue', 'count', 'win_percent', 'lose_percent', 'tie_percent']

        for arr_feature in stats_list:
            for feature in arr_feature:
                tablename = "stats_" + feature + "_"
            tablename += str(version)

            self.db.create_table(tablename, fieldlist)


            for record in stats_list[arr_feature]:
                value = ""
                for i in range(len(arr_feature)):
                    value += record[i] + ", "
                value += "'" + Times + "'," + str(isTop) + ", "
                for column in common_column:
                    value += stats_list[arr_feature][record][column] + ", "
                value = value[:-2]

                print(value)
                self.db.insert(tablename, value)






S = Stats()
'''
arr_paras = ["ComputeLantency","IntervalNum","inMuUpper","outMuUpper","StopDown","StopUp"]
result = S.stats_revenue("revenue20170516stop",arr_paras)
for key in result:
    para = key.split("_")

    print(str(para[0]) + "," + str(para[1]) + "," + str(para[2]) + "," + str(para[3]) + "," + str(para[4]) + "," + str(para[5])+ "," , end="" )
    print(str(result[key]['tradeNum']) + "," + str(result[key]['winNum'])  + "," +str(result[key]['win_percent'])  + "," +str(result[key]['avg_revenue']) + "," +str(result[key]['total_revenue']),end="" )
    print("," + str(result[key]['win_avg'])  + "," + str(result[key]['lose_avg']))
'''

#S.stats_inner_max("tradeinfos20170426", "data201306", "Max_DPrice20170511")
#S.stats_Dmax_Revenue("Max_DPrice20170510_1020")
S.stats_Dmax_Reveune_percent("Max_DPrice20170508","Max_DPrice20170511")


