from StockDataAnalysis.Utility.DB import DB
from StockDataAnalysis.Utility.PlotFigure import LineChart
from StockDataAnalysis.Utility.PlotFigure import Scatter
from StockDataAnalysis.Utility.PlotFigure import DoubleAxisLineChart
from StockDataAnalysis.Utility.PlotFigure import BoxFigure
import time
import os
import pandas as pd


class Visualization:
    start = 0
    offset = 10000
    def __init__(self):
        self.db = DB('localhost', 'stockresult','root','0910mysql@')

    def trend(self, tablename, Times, xsample, ysample, figname, path, **config):
        con = {'condition': "Times like '" + str(Times) +"%'"}
        record_num = self.db.select(tablename, "count(*)", **con)[0]['count(*)']
        y = []
        x = []
        while self.start < record_num:
            con = {'limit': {'start':self.start, 'offset':self.offset}}
            result = self.db.select(tablename,"*", **con)
            count = 0
            for re in result:
                if count % ysample == 0:
                    y.append(re['LastPrice'])
                    if count % xsample == 0:
                        x.append(re["Times"])
                    else:
                        x.append("")
                count += 1
            self.start += self.offset
        label = {
            'xlabel': "Times",
            'ylabel': "LastPrice"
        }
        title = "Trend_" + Times
        para = {
            'label': label,
            'title': title,
        }
        if 'line_para' in config:
            para.setdefault('line_para',config['line_para'])


        L = LineChart(**para)
        if 'fig_para' in config:
            L.set_figsize(**config['fig_para'])
        print(config['line_para'])
        L.plot_figure(x, y)
        L.save(figname, path)


    def log_trend(self, tablename, Times, xsample, ysample, figname, path, **line_para):
        con = {'condition': "Times like '" + str(Times) + "'%"}
        record_num = self.db.select(tablename, "count(*)", **con)
        y = []
        x = []
        while self.start < record_num:
            con = {'limit': {'start': self.start, 'offset': self.offset}}
            result = self.db.select(tablename, "*", **con)
            count = 0
            for re in result:
                if count % ysample == 0:
                    y.append(re['LastPrice'])
                    if count % xsample == 0:
                        x.append(re["Times"])
                    else:
                        x.append("")
                count += 1
        label = {
            'xlabel': "Times",
            'ylabel': "LastPrice"
        }
        title = "Trend_" + Times
        para = {
            'label': label,
            'title': title
        }
        if 'line_para' in line_para:
            para.setdefault('line_para', line_para['line_para'])
        L = LineChart(**para)
        L.plot_figure(x, y)
        L.save(figname, path)

    def get_one_day_Mu(self,tablename, day, db, arr_y, arr_x):
        sql = "select times, Mu from " + tablename + " where times like '" + day + "%'"
        db.cur.execute(sql)
        result = db.cur.fetchall()

        for re in result:
            arr_y.append(re['Mu'])
            arr_x.append(re['times'])


    def day_Mu(self,tablename, db, num):
        f = open("C:\\Users\\songxue\\Desktop\\Day.txt",'r')
        lines = f.readlines()

        for line in lines:
            print(line[:-1])
            arr_y = []
            arr_x = []
            day = line[:-1]
            self.get_one_day_Mu(tablename,day,db,arr_y,arr_x)
            F = LineChart(label={'ylabel':"Mu", 'xlabel':"Times"}, title= day + "_Mu", isGrid=False)
            F.set_figsize(figsize=(60,35))

            F.plot_figure(arr_x,arr_y)
            F.set_line_para({'color': "red"})
            F.plot_figure(arr_x[:num],arr_y[:num])
            F.save(day + "_Mu.png","C:\\Users\\songxue\\Desktop\\DayMu\\")

    def time_to_long(self,Times):
        L = time.mktime(time.strptime(Times[:17], '%Y%m%d-%H:%M:%S'))
        L = L + int(Times[18:]) * 0.001
        return L

    def long_to_time(self,L):
        times = time.localtime(L)
        return time.strftime('%Y%m%d-%H:%M:%S', times)

    def time_to_x(self,times, start):
        t = self.time_to_long(times)
        return t - start

    def isSample(self,times, sample,start):
        t1 = start
        L1 = self.time_to_long(t1)
        L2 = self.time_to_long(times)
        L = L2 - L1
        if L % sample == 0 and L>=0 :
            return True
        return False

    def find_nearest_time(self,times,sample,sample_times):
        L1 = self.time_to_long(times)
        L2 = int(L1/sample) * sample
        L3 = L2 + sample
        if (L3 - L1) < (L1 - L2):
            print(self.long_to_time(L3))
            while self.long_to_time(L3) not in sample_times:
                L3 += sample

            return sample_times[self.long_to_time(L3)]['pos'], sample_times[self.long_to_time(L3)]['LastPrice']
        else:
            print(self.long_to_time(L2))
            while self.long_to_time(L2) not in sample_times:
                L2 += sample

            return sample_times[self.long_to_time(L2)]['pos'], sample_times[self.long_to_time(L2)]['LastPrice']



    def trade_trend(self, tablename1, tablename2, db, day, start, end, sample, ComputeLantency, IntervalNum, MuUpper,lnPriceThreshold):
        result1 = db.select(tablename1, "*", condition="times like '" + day + "%'")
        arr_y = []
        arr_x = []
        sample_time = {}
        count = 0

        condition = "times like '" + day + "%' and ComputeLantency=" + str(ComputeLantency)
        condition += " and intervalNum=" + str(IntervalNum) + " and MuUpper=" + str(MuUpper)

        result2 = db.select(tablename2, "*", condition=condition)
        if len(result2) > 0:
            for re in result1:
                if self.isSample(re['Times'], sample, start):
                    arr_y.append(re['LastPrice'])

                    value = {'pos': count, 'LastPrice': re['LastPrice']}
                    sample_time.setdefault(re['Times'][:17], value)

                    if count % 10 == 0:
                        arr_x.append(re['Times'])
                    else:
                        arr_x.append("")


                    count += 1
                if self.time_to_long(re['Times']) > self.time_to_long(end):
                    break

            arr_point_in_long_x = []
            arr_point_in_long_y = []
            arr_point_in_short_x = []
            arr_point_in_short_y = []
            arr_point_out_long_x = []
            arr_point_out_long_y = []
            arr_point_out_short_x = []
            arr_point_out_short_y = []
            print(sample_time)
            for re in result2:
                x, y = self.find_nearest_time(re['Times'], sample, sample_time)
                if re['isOpen'] == 1:
                    if re['isLong'] == 1:
                        arr_point_in_long_x.append(x)
                        arr_point_in_long_y.append(y)

                    else:
                        arr_point_in_short_x.append(x)
                        arr_point_in_short_y.append(y)
                else:
                    if re['isLong'] == 1:
                        arr_point_out_long_x.append(x)
                        arr_point_out_long_y.append(y)

                    else:
                        arr_point_out_short_x.append(x)
                        arr_point_out_short_y.append(y)

            title = day + "_" + str(ComputeLantency) + "_" + str(IntervalNum) + "_" + str(MuUpper)
            L = LineChart(label={'xlabel': "Times", 'ylabel': "LastPrice"}, title=title, isGrid=False)
            L.set_figsize(figsize=(60, 35))
            L.plot_figure(arr_x, arr_y,1,45,40,15,30)
            S = Scatter()

            S.plot_figure(arr_point_in_long_x, arr_point_in_long_y, c="black", s=200, marker="+", label="In & Long")
            S.plot_figure(arr_point_out_long_x, arr_point_out_long_y, c="red", s=200, marker="+", label="Out & Long")
            S.plot_figure(arr_point_in_short_x, arr_point_in_short_y, c="black", s=100, marker="o", label="In & Short")
            S.plot_figure(arr_point_out_short_x, arr_point_out_short_y, c="red", s=100, marker="o", label="Out & Short")
            S.legend(loc='upper right', prop={'size': 30})

            path = "C:\\Users\\songxue\\Desktop\\" + str(tablename2) + "\\" + str(ComputeLantency) + "_" + str(
                IntervalNum) + "_" + str(MuUpper) + "_" + str(lnPriceThreshold) + "\\"
            if not os.path.exists(path):
                os.makedirs(path)
            figname = title + ".png"
            S.save(figname, path)


    def trade_trend(self, tablename1, tablename2, db, day, start, end, sample, ComputeLantency, IntervalNum, inMuUpper,outMuUpper,lnPriceThreshold):
        result1 = db.select(tablename1, "*", condition="times like '" + day + "%'")
        arr_y = []
        arr_x = []
        sample_time = {}
        count = 0

        condition = "OpenTimes like '" + day + "%' and ComputeLantency=" + str(ComputeLantency)
        condition += " and intervalNum=" + str(IntervalNum) + " and inMuUpper=" + str(inMuUpper) + " and outMuUpper=" + str(outMuUpper)

        result2 = db.select(tablename2, "*", condition=condition)
        if len(result2) > 0:
            for re in result1:
                if self.isSample(re['Times'], sample, start):
                    arr_y.append(re['LastPrice'])

                    value = {'pos': count, 'LastPrice': re['LastPrice']}
                    sample_time.setdefault(re['Times'][:17], value)

                    if count % 10 == 0:
                        arr_x.append(re['Times'])
                    else:
                        arr_x.append("")


                    count += 1
                if self.time_to_long(re['Times']) > self.time_to_long(end):
                    break

            arr_point_in_long_x = []
            arr_point_in_long_y = []
            arr_point_in_short_x = []
            arr_point_in_short_y = []
            arr_point_out_long_x = []
            arr_point_out_long_y = []
            arr_point_out_short_x = []
            arr_point_out_short_y = []
            print(sample_time)
            for re in result2:
                in_x, in_y = self.find_nearest_time(re['OpenTimes'], sample, sample_time)
                out_x, out_y = self.find_nearest_time(re['CloseTimes'], sample, sample_time)

                if re['isLong'] == 1:
                    arr_point_in_long_x.append(in_x)
                    arr_point_in_long_y.append(in_y)
                    arr_point_out_long_x.append(out_x)
                    arr_point_out_long_y.append(out_y)

                else:
                    arr_point_in_short_x.append(in_x)
                    arr_point_in_short_y.append(in_y)
                    arr_point_out_short_x.append(out_x)
                    arr_point_out_short_y.append(out_y)



            title = day + "_" + str(ComputeLantency) + "_" + str(IntervalNum) + "_" + str(inMuUpper) + "_" + str(outMuUpper)
            L = LineChart(label={'xlabel': "Times", 'ylabel': "LastPrice"}, title=title, isGrid=False)
            L.set_figsize(figsize=(60, 35))
            L.plot_figure(arr_x, arr_y,1,45,40,15,30)
            S = Scatter()

            S.plot_figure(arr_point_in_long_x, arr_point_in_long_y, c="black", s=200, marker="+", label="In & Long")
            S.plot_figure(arr_point_out_long_x, arr_point_out_long_y, c="red", s=200, marker="+", label="Out & Long")
            S.plot_figure(arr_point_in_short_x, arr_point_in_short_y, c="black", s=100, marker="o", label="In & Short")
            S.plot_figure(arr_point_out_short_x, arr_point_out_short_y, c="red", s=100, marker="o", label="Out & Short")
            S.legend(loc='upper right', prop={'size': 30})

            path = "C:\\Users\\songxue\\Desktop\\" + str(tablename2) + "\\" + str(ComputeLantency) + "_" + str(
                IntervalNum) + "_" + str(inMuUpper) + "_" + str(outMuUpper) + "_" + str(lnPriceThreshold) + "\\"
            if not os.path.exists(path):
                os.makedirs(path)
            figname = title + ".png"
            S.save(figname, path)


    def mintue_trend_mu(self,tablename1, tablename2, db, day, start, end, sample):
        result1 = db.select(tablename1, "*", condition="times like '" + day + "%'")
        arr_y = []
        arr_x = []
        count = 0
        pos_list = {}
        for re in result1:
            if self.isSample(re['Times'], sample, start):
                arr_y.append(re['LastPrice'])
                pos_list.setdefault(re['Times'], count)
                if count % 20 == 0:
                    arr_x.append(re['Times'])
                else:
                    arr_x.append("")
                count += 1
            if self.time_to_long(re['Times']) > self.time_to_long(end):
                break

        result2 = db.select(tablename2, "Times,Mu", condition="times like '" + day + "%'")
        arr_z = []
        arr_zx = []
        for re in result2:
            if self.isSample(re['Times'], sample, start):
                arr_zx.append(pos_list[re['Times']])
                arr_z.append(re['Mu'])
            if self.time_to_long(re['Times']) > self.time_to_long(end):
                break
        print(arr_zx)

        '''
        f = open("C:\\Users\\songxue\\Desktop\\otherMu.txt", 'r')
        lines = f.readlines()

        arr_z1 = []
        for line in lines:
            line = line.split('\t')
            times = line[0]
            print(times)
            if self.isSample(times, sample, start):
                arr_z1.append(float(line[1][:-1]))
            if self.time_to_long(times) > self.time_to_long(end):
                break
        print(len(arr_z1))
        print(len(arr_z))
        '''

        title = "Trend_Mu_" + day
        D = DoubleAxisLineChart()
        D.set_figsize((60, 35))
        D.plot_first_axis(arr_x, arr_y, "LastPrice", title, "times", 2, 70, 50, 30, 50)
        D.plot_second_axis(arr_zx,arr_z, "Mu", 'r', 2, 50, 50)
        #D.plot_second_axis(arr_z1, "", 'black', 10, 50, 50)
        figname = title + ".png"
        path = "C:\\Users\\songxue\\Desktop\\"
        D.save(figname, path)

    def get_mu(self,tablename1,db,day,start,sample):
        time_list = []
        f = open("C:\\Users\\songxue\\Desktop\\Times.txt")
        lines = f.readlines()

        for line in lines:
            time_list.append(line[:-1])

        for time in time_list:
            sql = "select * from " + tablename1 + " where Times='" + time +"'"
            db.cur.execute(sql)
            result = db.cur.fetchall()
            if len(result)>0:
                re= result[0]
                print(re['Times'] + ",", end='')
                print(re['LastPrice'])
            else:
                print("Not match Time")

        '''
        for re in result1:
            if self.isSample(re['Times'], sample, start):
                print(re['Times'] + ",", end='')
                print(re['LastPrice'])
        '''

    def Dmax(self,stats_table,path):
        C_I_result = self.db.select(stats_table, "distinct ComputeLantency,IntervalNum")
        sql1 = "select distinct inMuUpper,outMuUpper from " + stats_table + " order by inMuUpper,outMuUpper"
        self.db.cur.execute(sql1)
        Mu_result = self.db.cur.fetchall()
        arr_type = ["Revenue>0","Revenue<0"]
        name = "win"
        for type in arr_type:

            for re in C_I_result:
                DF = pd.DataFrame()

                for mu in Mu_result:
                    sql = "select Dmax from " + stats_table + " where ComputeLantency=" + str(
                        re['ComputeLantency']) + " and IntervalNum="
                    sql += str(re['IntervalNum']) + " and inMuUpper=" + str(mu['inMuUpper']) + " and outMuUpper=" + str(
                        mu['outMuUpper'])

                    sql += " and " + type
                    #print(sql)
                    df = pd.read_sql(sql, self.db.conn)
                    col_name = str(mu['inMuUpper']) + "_" + str(mu['outMuUpper'])
                    DF[col_name] = df

                #print(DF)
                Ymin = DF.min()
                ymin = int(Ymin.min())-1
                Ymax = DF.max()
                ymax = int(Ymax.max())+1
                print(ymin)
                print(ymax)
                box = BoxFigure(DF)
                box.sns_plot(ymin,ymax)
                figname = stats_table + "_" + name + "_" + str(re['ComputeLantency']) + "_" + str(re['IntervalNum']) + ".png"
                path1 = path + name + "\\"
                box.save(figname, path1)

            name = "lose"



db = DB('localhost', 'stockresult','root','0910mysql@')
V = Visualization()
#V.mintue_trend_mu("data201306", "innervaluemu",db,"20130625","20130625-09:45:00 0", "20130625-10:15:00 0", 20)


table_list = ["Max_DPrice20170511"]
for table in table_list:
    isLog = False
    day_list = []
    sql = "SELECT distinct mid(OpenTimes,1,8) FROM "+ table
    db.cur.execute(sql)
    result = db.cur.fetchall()
    for re in result:
        day_list.append(re['mid(OpenTimes,1,8)'])
    print(day_list)
    sql = "SELECT distinct ComputeLantency,inMuUpper,outMuUpper FROM " + table + " where ComputeLantency=5 or ComputeLantency=10 or ComputeLantency=20 or ComputeLantency=30"
    db.cur.execute(sql)
    result = db.cur.fetchall()
    for re in result:
        for day in day_list:
            start = day + "-09:00:00 0"
            end = day + "-16:00:00 0"
            V.trade_trend("data201306",table,db,day,start,end,20,re['ComputeLantency'],"20",re['inMuUpper'],re['outMuUpper'],"0.0001")

#V.trade_trend("data201306","tradeinfoschangepoint",db,"20130625","20130625", "20130625-16:00:00 0",20,20,20,0.0002)


#V.mintue_trend_mu("data201306","innervaluemu",db,"201306","20130603-09:00:00 0", "20130628-16:00:00 0",480)
#V.get_mu("data201306",db,"201306","20130603-09:00:00 0", 10)
#path = "C:\\Users\\songxue\\Desktop\\Dmax_2010\\"
#V.Dmax("Max_DPrice20170510_1020",path)
