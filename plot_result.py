#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymysql
import matplotlib.pyplot as plt
from matplotlib import cm
import math
import os
import random
import csv
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

def plot_sub_figure(x,y,title,xlabel,ylabel, auto,linewidth, xfontsize, labelsize):
    if auto:
        plt.plot(y)
        plt.xticks(range(len(x)), x, rotation=45)
        plt.grid()
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

    else:
        plt.plot(y, linewidth=linewidth)
        plt.yticks(fontsize=40)
        plt.xticks(range(len(x)), x, rotation=90, fontsize=xfontsize)
        plt.title(title, fontsize=45)
        plt.grid()
        plt.xlabel(xlabel, fontsize=labelsize)
        plt.ylabel(ylabel, fontsize=labelsize)
        plt.axis(fontsize=30)



def plot_figure(x,y,isTop):
    arr_title = ["Month Total Revenue", "Month Average Revenue", "Month Trade Num", "Month Win Percent", "Month Lose Percent", "Month Tie Percent" ]
    arr_ylabel = ["Total Revenue", "Average Revenue", "Trade Num", "Win Percent", "Lose Percent", "Tie Percent"]
    y_key = ['total_revenue', 'average_revenue', 'count', 'win_percent', 'lose_percent', 'tie_percent']
    plt.figure(figsize=(60, 35))
    for i in range(1,7):
        plt.subplot(2,3,i)
        plot_sub_figure(x,y[y_key[i-1]],arr_title[i-1],"Month",arr_ylabel[i-1],False,10,20,45)

    path = "C:\\Users\\songxue\\Desktop\\stats_info\\"
    figname = "Year stats " + str(isTop) + ".png"
    plt.savefig(path + figname, dpi=100)
    plt.close()


def plot_feature_figure(x,arr_dict_y,xlabel,ylabel,isTop, linewidth, xfontsize, labelsize,month_list):
    arr_title = month_list

    plt.figure(figsize=(60, 35))
    for i in range(1,14):
        plt.subplot(3,5,i)
        plt.subplots_adjust(left=0.06, right=0.98,top=0.96,bottom=0.09,wspace=0.35,hspace=0.55)
        plot_sub_figure(x[i-1],arr_dict_y[i-1][ylabel],arr_title[i-1],xlabel,ylabel,False,linewidth, xfontsize, labelsize )

    #plt.show()

    path = "C:\\Users\\songxue\\Desktop\\stats_info\\new1\\" + xlabel
    if not os.path.exists(path):
        os.makedirs(path)
    figname = xlabel + "_" + ylabel + "_" + str(isTop) + ".png"
    path += "\\"
    plt.savefig(path+figname, dpi=100)
    plt.close()

def plot_feature_figure_all(x,arr_dict_y,xlabel,ylabel,isTop, linewidth, xfontsize, labelsize):
    arr_x = []
    arr_y = []
    for i in range(0,12):
        for m in x[i]:
            arr_x.append(m)
        for n in arr_dict_y[i][ylabel]:
            arr_y.append(n)


    plt.figure(figsize=(60, 35))
    fig_title = "Month " + xlabel + " " + ylabel

    plot_sub_figure(arr_x, arr_y, fig_title, xlabel, ylabel, False,linewidth, xfontsize, labelsize)

    # plt.show()

    path = "C:\\Users\\songxue\\Desktop\\stats_info\\new1\\" +"all_" + xlabel
    if not os.path.exists(path):
        os.makedirs(path)
    figname = "all_" + xlabel + "_" + ylabel + "_" + str(isTop) + ".png"
    path += "\\"
    plt.savefig(path + figname, dpi=100)
    plt.close()

def plot_3d_color(x,y,z,color_value,xlabel,ylabel,zlabel,color_name,title,isTop):
    fig = plt.figure()
    ax = fig.gca(projection='3d')


    # Plot the surface.
    surf = ax.plot_surface(x,y,z, cmap=cm.coolwarm,
                           linewidth=0, antialiased=False)



    # Add a color bar which maps values to colors.
    fig.colorbar(surf, shrink=0.5, aspect=5)

    plt.show()

    return

def plot_c_i_m(arr_C_I_M,arr_color_value,color_name,isTop):
    arr_title = ["2013", "201303", "201304", "201305", "201306", "201307", "201308", "201309", "201310", "201311", "201312"]

    for i in range(0,11):
        arr_C = []
        arr_I = []
        arr_M = []
        for C_I_M in arr_C_I_M[i]:
            s = C_I_M.split("_")
            arr_C.append(s[0])
            arr_I.append(s[1])
            arr_M.append(s[2])

        plot_3d_color(arr_C,arr_I,arr_M,arr_color_value[i],"ComputeLantency","IntervalNum","MuUpper",color_name,arr_title[i],isTop)

    return



def get_stats_data(cur, range):
    if range == "all":
        sql = "select * from stats where isTop = 0"
    else:
        sql = "select * from stats where isTop = 1"

    cur.execute(sql)
    result = cur.fetchall()
    return result

def get_feature_data(cur,isTop,feature, times):

    if feature == "ComputeLantency":
        tablename = "stats_computelantency"
    elif feature == "IntervalNum":
        tablename = "stats_intervalnum"
    elif feature == "MuUpper":
        tablename = "stats_muupper"
    elif feature == "Mu":
        tablename = "stats_Mu"
    elif feature == "Sigma":
        tablename = "stats_Sigma"
    elif feature == "LossingStopUp":
        tablename = "stats_LossingStopUp"
    else:
        tablename = "stats_c_i_m"

    sql = "select * from " + tablename + " where isTop=" + str(isTop) + " and Times=" + str(times)
    cur.execute(sql)
    result = cur.fetchall()
    return result

def plot_month(result, isTop, month_list):
    arr_x = []
    dict_y = {'total_revenue': [], 'average_revenue': [], 'count': [], 'win_percent': [], 'lose_percent': [],
              'tie_percent': []}
    for re in result:
        if re['Times'] != "2013":
            arr_x.append(re['Times'][-2:])
            dict_y['total_revenue'].append(re['total_revenue'])
            dict_y['average_revenue'].append(re['average_revenue'])
            dict_y['count'].append(re['count'])
            dict_y['win_percent'].append(re['win_percent'])
            dict_y['lose_percent'].append(re['lose_percent'])
            dict_y['tie_percent'].append(re['tie_percent'])

    plot_figure(arr_x, dict_y,isTop)

def plot_feature(cur, feature,isTop,month_list):
    time = month_list

    arr_dict_y = []
    arr_x = []

    for t in time:
        result = get_feature_data(cur, isTop, feature, t)
        x = []
        dict_y = {'total_revenue': [], 'average_revenue': [], 'count': [], 'win_percent': [], 'lose_percent': [],
                  'tie_percent': []}

        for re in result:
            x.append(re[feature])
            dict_y['total_revenue'].append(re['total_revenue'])
            dict_y['average_revenue'].append(re['average_revenue'])
            dict_y['count'].append(re['count'])
            dict_y['win_percent'].append(re['win_percent'])
            dict_y['lose_percent'].append(re['lose_percent'])
            dict_y['tie_percent'].append(re['tie_percent'])
        arr_dict_y.append(dict_y)
        arr_x.append(x)


    y_key = ['total_revenue', 'average_revenue', 'count', 'win_percent', 'lose_percent', 'tie_percent']
    if feature == "C_I_M":
        for j in range(0, 6):
            plot_c_i_m(arr_x,arr_dict_y,y_key[j],isTop)
    else:
        for j in range(0, 6):
            plot_feature_figure(arr_x, arr_dict_y, feature, y_key[j], isTop, 10, 30, 45, month_list)
            plot_feature_figure_all(arr_x, arr_dict_y, feature, y_key[j], isTop, 10, 20, 45)


def get_result_db(cur,sql):
    cur.execute(sql)
    return cur.fetchall()

def plot_trend(cur,month_list):
    arr_month = month_list

    dict_x = {}
    dict_y = {}
    arr_x = []
    arr_y = []
    count_year = 0
    for month in arr_month:
        tablename = "data" + month
        sql = "select * from " + tablename
        result = get_result_db(cur,sql)
        dict_x.setdefault(month,[])
        dict_y.setdefault(month,[])
        count_month = 0
        for re in result:
            if count_year % 6000 == 0:
                arr_x.append(re['Times'])
                arr_y.append(re['LastPrice'])

            if count_month % 6000 == 0:
                dict_x[month].append(re['Times'])
                dict_y[month].append(re['LastPrice'])

            count_year += 1
            count_month += 1

    for month in arr_month:
        for i in range(len(dict_x[month])):
            if i % 10 != 0:
                dict_x[month][i] = ""
    for i in range(len(arr_x)):
        if i % 100 != 0:
            arr_x[i] = ""




    print("start to figure")

    '''
    x = ["" for i in range(len(arr_x))]

    for i in range(0,20):
        x[i*10000] = dict_x["201303"][i*10000]
    print(x[0:10])
    print(x[20000])

    plt.plot(dict_y["201303"])
    print("y plt finished")
    plt.xticks(range(30000), x[0:30000], rotation=30)
    plt.show()
    '''



    plt.figure(figsize=(60, 35))
    for i in range(1,13):
        plt.subplot(3,4,i)
        plt.subplots_adjust(left=0.06, right=0.98, top=0.96, bottom=0.09, wspace=0.35, hspace=0.55)
        print("start to plot subfigure" + str(i))
        plot_sub_figure(dict_x[arr_month[i-1]], dict_y[arr_month[i-1]], arr_month[i-1], "Times", "LastPrice",False,10,20,20)


    path = "C:\\Users\\songxue\\Desktop\\stats_info\\"
    figname = "month_trend.png"
    plt.savefig(path + figname, dpi=100)
    plt.close()

    print("start to plot all figure")
    plt.figure(figsize=(60, 35))
    plot_sub_figure(arr_x,arr_y,"Year Trend","Times", "LastPrice",False,10,20,20)
    figname = "year_trend.png"
    plt.savefig(path + figname, dpi=100)
    plt.close()

def plot_log_trend(cur,month_list):
    arr_month = month_list

    dict_x = {}
    dict_log_d_value = {}
    dict_log_d_value_sum = {}
    arr_x = []
    arr_log_d_value = []
    arr_log_d_value_sum = []
    count_year = 1
    log_sum_year = 0
    for month in arr_month:
        tablename = "data" + month
        sql = "select * from " + tablename
        result = get_result_db(cur, sql)
        dict_x.setdefault(month, [])
        dict_log_d_value.setdefault(month, [])
        dict_log_d_value_sum.setdefault(month,[])
        count_month = 1
        log_sum_month = 0
        before_price = result[0]['LastPrice']
        for re in result[1:]:
            log_d_value = math.log(float(re['LastPrice'])) - math.log(float(before_price))
            log_sum_month += log_d_value
            log_sum_year += log_d_value

            if count_year % 6000 == 0:
                arr_x.append(re['Times'])
                arr_log_d_value.append(log_d_value)
                arr_log_d_value_sum.append(log_sum_year)

            if count_month % 6000 == 0:
                dict_x[month].append(re['Times'])
                dict_log_d_value[month].append(log_d_value)
                dict_log_d_value_sum[month].append(log_sum_month)

            count_year += 1
            count_month += 1
            before_price = re['LastPrice']

    for month in arr_month:
        for i in range(len(dict_x[month])):
            if i % 10 != 0:
                dict_x[month][i] = ""
    for i in range(len(arr_x)):
        if i % 100 != 0:
            arr_x[i] = ""

    print("start to figure")


    dict_y = dict_log_d_value
    arr_y = arr_log_d_value
    for i in range(0, 2):
        print("i:" + str(i))
        if i == 0:
            ylabel = "log_d_value"
        else:
            ylabel = "log_d_value_sum"
        plt.figure(figsize=(60, 35))
        for i in range(1, 13):
            plt.subplot(3, 4, i)
            plt.subplots_adjust(left=0.06, right=0.98, top=0.96, bottom=0.09, wspace=0.35, hspace=0.55)
            print("start to plot subfigure" + str(i))
            plot_sub_figure(dict_x[arr_month[i - 1]], dict_y[arr_month[i - 1]], arr_month[i - 1], "Times", ylabel,
                            False, 10, 20, 20)

        path = "C:\\Users\\songxue\\Desktop\\stats_info\\"
        figname = "month_" + ylabel + "_trend.png"
        plt.savefig(path + figname, dpi=100)
        plt.close()

        print("start to plot all figure")
        plt.figure(figsize=(60, 35))
        plot_sub_figure(arr_x, arr_y, "Year Trend", "Times", ylabel, False, 10, 20, 20)
        figname = "year_"+ ylabel + "_trend.png"

        plt.savefig(path + figname, dpi=100)
        plt.close()

        dict_y = dict_log_d_value_sum
        arr_y = arr_log_d_value_sum

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

    for day in good_day:
        if day in bad_day and day in average_day:
            C_I_M = []
            C_I_M.append(good_day[day])
            C_I_M.append(bad_day[day])
            C_I_M.append(average_day[day])
            candidate_day.setdefault(day,C_I_M)

    return candidate_day

def get_C_I_M(str_c_i_m):
    C_I_M = str_c_i_m.split("_")
    return C_I_M


def plot_one_day_trade(cur,day,tablename2, Computelantency, IntervalNum, MuUpper, type, isLog,path,**InnerValue):
    tablename1 = "data" + day[0:6]
    sql1 = "select * from " + tablename1 + " where Times like '" + day + "%' "
    cur.execute(sql1)
    result1 = cur.fetchall()

    sql2 = "select * from " + tablename2 + " where Times like '" + day + "%'"
    sql2 += " and Computelantency=" + Computelantency + " and IntervalNum=" + IntervalNum + " and MuUpper=" + MuUpper

    cur.execute(sql2)
    result2 = cur.fetchall()
    point_list = {}

    for re in result2:
        value = {'position':0, 'LastPrice':re['LastPrice'],'isOpen':re['isOpen'],'isLong':re['isLong']}
        point_list.setdefault(re['Times'], value)
    #print(point_list)

    arr_y = []
    arr_x = []
    position = 0
    count = 0

    if 'InnerValue' in InnerValue:
        arr_z = []
        for re in result2[1:]:
            arr_z.append(re[InnerValue['InnerValue']])

    if isLog:
        before_price = result1[0]['LastPrice']
        for re in result1[1:]:
            if re['Times'] in point_list:
                point_list[re['Times']]['position'] = position
                log_d_value = math.log(float(re['LastPrice'])) - math.log(float(before_price))
                point_list[re['Times']]['LastPrice'] = log_d_value
                arr_y.append(log_d_value)
                arr_x.append(re['Times'][9:])
                position += 1
                print(position)
            elif count % 300 == 0:
                print(float(re['LastPrice']))
                print(before_price)
                log_d_value = math.log(float(re['LastPrice'])) - math.log(float(before_price))
                print(log_d_value)
                arr_y.append(log_d_value)
                if count % 900 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                position += 1
            count += 1
            before_price = re['LastPrice']
    else:
        for re in result1:
            if re['Times'] in point_list:
                point_list[re['Times']]['position'] = position
                arr_y.append(re['LastPrice'])
                arr_x.append(re['Times'][9:])
                position += 1
                print(position)
            elif count % 300 == 0:
                arr_y.append(re['LastPrice'])
                if count % 900 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                position += 1
            count += 1



    print("start to plot trend")

    '''
    if isInnerValue:
        sql3 = "select * from innervalue2day where ComputeLantency=" + str(Computelantency) + " and " \
               "IntervalNum=" + str(IntervalNum) + " and MuUpper=" + str(MuUpper)
        cur.execute(sql3)
        result3 = cur.fetchall()[0]
        title += "Mu:" + result3['Mu'] + "Sigma:" + result3['Sigma'] + "LossingStopUp:" + result3['LossingStopUp']
    '''

    if isLog:
        ylabel = "LastPrice_log_d_value"
    else:
        ylabel = "LastPrice"
    print(ylabel)
    title = day + "_" + type + "_" + Computelantency + "_" + IntervalNum + "_" + MuUpper
    if 'InnerValue' in InnerValue:
        fig,ax1 = plt.subplots(figsize=(60, 35))
        ax2 = ax1.twinx()
        ax2.plot(arr_z,'yellow')
        ax2.set_label(InnerValue['InnerValue'])
        title += InnerValue['InnerValue']
    else:
        plt.figure(figsize=(60, 35))

    plot_sub_figure(arr_x,arr_y, title, "Times",ylabel , False, 5, 35, 45)


    print("finish plot trend and start to plot point")

    arr_point_in_long_x = []
    arr_point_in_long_y = []
    arr_point_in_short_x = []
    arr_point_in_short_y = []
    arr_point_out_long_x = []
    arr_point_out_long_y = []
    arr_point_out_short_x = []
    arr_point_out_short_y = []

    crisis_data = []

    for point in point_list:
        data = (point_list[point]['position'], point_list[point]['LastPrice'])
        label = '(' + str(point[9:]) + ', ' + str(point_list[point]['LastPrice']) + ')'
        #crisis_data.append((data,label) )

        if point_list[point]['isOpen'] == 1:
            if point_list[point]['isLong'] == 1:
                arr_point_in_long_x.append(point_list[point]['position'])
                arr_point_in_long_y.append(point_list[point]['LastPrice'])

            else:
                arr_point_in_short_x.append(point_list[point]['position'])
                arr_point_in_short_y.append(point_list[point]['LastPrice'])
            color = "black"
            shift = 0.005
            text = (int(data[0] * 1.1), float(data[1] * (1 + shift)))
            horizontalalignment = 'left'
            verticalalignment = 'top'
            #plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
             #            horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)

        else:
            if point_list[point]['isLong'] == 1:
                arr_point_out_long_x.append(point_list[point]['position'])
                arr_point_out_long_y.append(point_list[point]['LastPrice'])
            else:
                arr_point_out_short_x.append(point_list[point]['position'])
                arr_point_out_short_y.append(point_list[point]['LastPrice'])
            color = "red"
            shift = 0.01
            text = (int(data[0] * 0.9), float(data[1] * (1 - shift)))
            horizontalalignment = 'right'
            verticalalignment = 'bottom'
            #plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
                #         horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)

    plt.scatter(arr_point_in_long_x,arr_point_in_long_y,c="black", s=500, marker="+",label="In & Long")
    plt.scatter(arr_point_out_long_x, arr_point_out_long_y, c="red", s=500, marker="+",label="Out & Long")
    plt.scatter(arr_point_in_short_x, arr_point_in_short_y, c="black", s=500, marker="o", label="In & Short")
    plt.scatter(arr_point_out_short_x, arr_point_out_short_y, c="red", s=500, marker="o", label="Out & Short")
    plt.legend(loc='upper right',prop={'size':45})



    figname = title + ".png"
    plt.savefig(path + figname, dpi=100)
    plt.close()

def plot_one_day_trade_multi(cur, day, tablename, ComputeLantency, IntervalNum, MuUpper, type,isLog, **InnerValue):
    if 'isLoop' in InnerValue:
        C_list = [5,6,7,8,9,10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22,23,24,25]
        I_list = [60, 120, 180, 240, 320, 480, 660, 720, 780, 840]
        M_list = [0.001, 0.0001, 0.00001, 0.000001, 0.0005, 0.00005, 0.000005, 0.0000005, 0]
    else:
        C_list = [10,11,12,13,14,15,16,17,18,19,20]
        I_list = [30,60,120,180,240,320,640]
        M_list = [0.001,0.0001,0.00001,0.000001,0.0005,0.00005,0.000005,0.0000005,0]
    path = "C:\\Users\\songxue\\Desktop\\stats_info\\"
    path += day + "_multi_feature\\" + type +"\\"
    C_path = path + "ComputeLantency\\"
    if not os.path.exists(C_path):
        os.makedirs(C_path)
    for c in C_list:
        plot_one_day_trade(cur,day,tablename,str(c),IntervalNum,MuUpper,type,isLog,C_path,InnerValue)

    I_path = path + "IntervalNum\\"
    if not os.path.exists(I_path):
        os.makedirs(I_path)
    for i in I_list:
        plot_one_day_trade(cur, day, tablename, ComputeLantency, str(i), MuUpper, type,isLog,I_path,InnerValue)

    M_path = path + "MuUpper\\"
    if not os.path.exists(M_path):
        os.makedirs(M_path)
    for m in M_list:
        plot_one_day_trade(cur, day, tablename, ComputeLantency, IntervalNum, str(m), type, isLog,M_path,InnerValue)

def output_InnerValue_file(cur,day,tablename,ComputeLantency, IntervalNum, MuUpper,type,path):
    sql = "select distinct Mu,Sigma,LossingStopUp, LossingStopDown from " + tablename+" where Times like '" + day + "%' " \
          "and ComputeLantency=" + str(ComputeLantency) + " and IntervalNum=" + str(IntervalNum) + " and MuUpper=" + str(MuUpper)
    cur.execute(sql)
    result = cur.fetchall()
    '''
    filename1 = path+ day + "_" + type + "_" + ComputeLantency + "_" + IntervalNum + "_" + MuUpper + "_InnerValue"
    if os.path.exists(filename1):
        os.remove(filename1)
    '''
    filename = path+ day + "_" + type + "_" + ComputeLantency + "_" + IntervalNum + "_" + MuUpper + "_InnerValue.csv"
    data = []
    Mu = []
    Sigma = []
    LossingStopUp = []
    LossingStopDown = []
    print(len(result))
    if len(result) > 0:
        for re in result:
            Mu.append(re['Mu'])
            Sigma.append(re['Sigma'])
            LossingStopUp.append(re['LossingStopUp'])
            LossingStopDown.append(re['LossingStopDown'])

        for mu, sigma, LossingStopUp, LossingStopDown in zip(Mu, Sigma, LossingStopUp, LossingStopDown):
            row = {
                'Mu': mu,
                'Sigma': sigma,
                'LossingStopUp': LossingStopUp,
                'LossingStopDown': LossingStopDown

            }
            data.append(row)

        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Mu', 'Sigma', 'LossingStopUp', 'LossingStopDown']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)


def getInnerValue(cur,day,tablename,ComputeLantency, IntervalNum, MuUpper, type,isLog):
    C_list = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
    I_list = [60, 120, 180, 240, 320, 480, 660, 720, 780, 840]
    M_list = [0.001, 0.0001, 0.00001, 0.000001, 0.0005, 0.00005, 0.000005, 0.0000005, 0]
    path = "C:\\Users\\songxue\\Desktop\\stats_info\\Mu_Sigma_LossingStopUp"
    if isLog:
        path += "log\\"
    path += day + "_multi_feature\\" + type + "\\"
    C_path = path + "ComputeLantency\\"
    if not os.path.exists(C_path):
        os.makedirs(C_path)
    for c in C_list:
        output_InnerValue_file(cur, day, tablename, str(c), IntervalNum, MuUpper, type, C_path)

    I_path = path + "IntervalNum\\"
    if not os.path.exists(I_path):
        os.makedirs(I_path)
    for i in I_list:
        output_InnerValue_file(cur, day, tablename, ComputeLantency, str(i), MuUpper, type, I_path)

    M_path = path + "MuUpper\\"
    if not os.path.exists(M_path):
        os.makedirs(M_path)
    for m in M_list:
        output_InnerValue_file(cur, day, tablename, ComputeLantency, IntervalNum, str(m), type, M_path)

def time_to_long(Times):
    L = time.mktime(time.strptime(Times[:17],'%Y%m%d-%H:%M:%S'))
    L = L + int(Times[18:])*0.001
    return L

def find_position(Times, position_list):
    t = time_to_long(Times)
    print(t)
    min_distance = 1000000
    min_pos = -1
    for pos in position_list:
        distance = math.fabs(t - pos)
        if distance < min_distance:
            min_distance = distance
            min_pos = pos
    return position_list[min_pos]['position'], position_list[min_pos]['LastPrice']



def plot_one_hour_trade(cur,hour,tablename2, Computelantency, IntervalNum, MuUpper, type, isLog,path,**InnerValue):

    tablename1 = "data" + hour[0:6]
    sql1 = "select * from " + tablename1 + " where Times like '" + hour + "%' "
    cur.execute(sql1)
    result1 = cur.fetchall()
    if hour[10] == "9":
        result1 = result1[2:]

    sql2 = "select * from " + tablename2 + " where Times like '" + hour + "%'"
    sql2 += " and Computelantency=" + Computelantency + " and IntervalNum=" + IntervalNum + " and MuUpper=" + MuUpper

    cur.execute(sql2)
    result2 = cur.fetchall()
    point_list = {}

    for re in result2:
        value = {'position': 0, 'LastPrice': re['LastPrice'], 'isOpen': re['isOpen'], 'isLong': re['isLong']}
        point_list.setdefault(re['Times'], value)


    arr_y = []
    arr_x = []
    position = 0
    count = 0

    position_list = {}

    if 'InnerValue' in InnerValue:
        arr_z = []
        for re in result2[1:]:
            arr_z.append(re[InnerValue['InnerValue']])

    if isLog:
        before_price = result1[0]['LastPrice']
        for re in result1[1:]:
            if count % 20 == 0:
                print(float(re['LastPrice']))
                print(before_price)
                log_d_value = math.log(float(re['LastPrice'])) - math.log(float(before_price))
                print(log_d_value)
                arr_y.append(log_d_value)
                if count % 100 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                value = {'position': position, 'LastPrice': log_d_value}
                position_list.setdefault(time_to_long(re['Times']), value)
                position += 1

            count += 1
            before_price = re['LastPrice']
    else:
        for re in result1:
            print("---------------------------------------")
            print(position)
            if count % 20 == 0:
                arr_y.append(re['LastPrice'])
                if count % 100 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                '''
                if position in range(15,20):
                    print("Times Long Position:----------------------------------------")
                    print(re['Times'])
                    print(time_to_long(re['Times']))
                    print(position)
                    if time_to_long(re['Times']) in position_list:
                        print("This time exists")
                '''
                value = {'position':position, 'LastPrice':re['LastPrice']}
                position_list.setdefault(time_to_long(re['Times']), value)
                position += 1
            count += 1

    print("start to plot trend")



    for pos in position_list:
        print(str(pos) + "  ")
        print(position_list[pos])




    if isLog:
        ylabel = "LastPrice_log_d_value"
    else:
        ylabel = "LastPrice"
    print(ylabel)
    title = tablename2 + "_" + hour + "_" + type + "_" + Computelantency + "_" + IntervalNum + "_" + MuUpper
    if 'InnerValue' in InnerValue:
        fig, ax1 = plt.subplots(figsize=(60, 35))
        ax2 = ax1.twinx()
        ax2.plot(arr_z, 'yellow')
        ax2.set_label(InnerValue['InnerValue'])
        title += InnerValue['InnerValue']
    else:
        plt.figure(figsize=(60, 35))

    plot_sub_figure(arr_x, arr_y, title, "Times", ylabel, False, 5, 35, 45)

    print("finish plot trend and start to plot point")

    arr_point_in_long_x = []
    arr_point_in_long_y = []
    arr_point_in_short_x = []
    arr_point_in_short_y = []
    arr_point_out_long_x = []
    arr_point_out_long_y = []
    arr_point_out_short_x = []
    arr_point_out_short_y = []

    crisis_data = []


    for point in point_list:
        point_list[point]['position'], point_list[point]['LastPrice'] = find_position(point, position_list)

        if point_list[point]['position'] == -1:
            print("Not find position !!")
            break

        data = (point_list[point]['position'], point_list[point]['LastPrice'])
        label = '(' + str(point[9:]) + ', ' + str(point_list[point]['LastPrice']) + ')'
        # crisis_data.append((data,label) )

        if point_list[point]['isOpen'] == 1:
            if point_list[point]['isLong'] == 1:
                arr_point_in_long_x.append(point_list[point]['position'])
                arr_point_in_long_y.append(point_list[point]['LastPrice'])

            else:
                arr_point_in_short_x.append(point_list[point]['position'])
                arr_point_in_short_y.append(point_list[point]['LastPrice'])
            color = "black"
            shift = 0.005
            text = (int(data[0] * 1.1), float(data[1] * (1 + shift)))
            horizontalalignment = 'left'
            verticalalignment = 'top'
            # plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
            #            horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)

        else:
            if point_list[point]['isLong'] == 1:
                arr_point_out_long_x.append(point_list[point]['position'])
                arr_point_out_long_y.append(point_list[point]['LastPrice'])
            else:
                arr_point_out_short_x.append(point_list[point]['position'])
                arr_point_out_short_y.append(point_list[point]['LastPrice'])
            color = "red"
            shift = 0.01
            text = (int(data[0] * 0.9), float(data[1] * (1 - shift)))
            horizontalalignment = 'right'
            verticalalignment = 'bottom'
            # plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
            #         horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)

    plt.scatter(arr_point_in_long_x, arr_point_in_long_y, c="black", s=500, marker="+", label="In & Long")
    plt.scatter(arr_point_out_long_x, arr_point_out_long_y, c="red", s=500, marker="+", label="Out & Long")
    plt.scatter(arr_point_in_short_x, arr_point_in_short_y, c="black", s=100, marker="o", label="In & Short")
    plt.scatter(arr_point_out_short_x, arr_point_out_short_y, c="red", s=100, marker="o", label="Out & Short")
    plt.legend(loc='upper right', prop={'size': 45})

    figname = title + ".png"
    plt.savefig(path + figname, dpi=100)
    plt.close()


def get_relative_position(arr, relative):
    temp = []
    for x in arr:
        temp.append(x-relative)
    return temp

def get_part_arr_point(arr_y,arr_x,iteration,length_x):
    list_x = [[] for i in range(iteration)]
    list_y = [[] for i in range(iteration)]
    print(list_x)
    for i in range(iteration):
        for j in range(len(arr_x)):
            print("j:" + str(j))
            print(arr_x[j])
            if arr_x[j] > int(i*(length_x/iteration)) and arr_x[j] < int((i+1)*(length_x/iteration)):
                list_x[i].append(arr_x[j]-int(i*(length_x/iteration)))
                list_y[i].append(arr_y[j])
    print(list_x)
    return list_x,list_y

def plot_minute_trade(cur,hour, minute, tablename2, Computelantency, IntervalNum, MuUpper, lnLastPriceThreshold, type, isLog,path, **InnerValue):
    tablename1 = "data" + hour[0:6]
    sql0 = "select min(LastPrice), max(LastPrice) from " + tablename1
    cur.execute(sql0)
    result0 = cur.fetchall()
    minLastPrice = result0[0]['min(LastPrice)']
    maxLastPrice = result0[0]['max(LastPrice)']
    sql1 = "select * from " + tablename1 + " where Times like '" + hour + "%' "
    cur.execute(sql1)
    result1 = cur.fetchall()
    if hour[10] == "9":
        result1 = result1[2:]

    sql2 = "select * from " + tablename2 + " where Times like '" + hour + "%'"
    sql2 += " and Computelantency=" + Computelantency + " and IntervalNum=" + IntervalNum + " and InMuUpper=" + MuUpper

    cur.execute(sql2)
    result2 = cur.fetchall()
    point_list = {}

    for re in result2:
        value = {'position': 0, 'LastPrice': re['LastPrice'], 'isOpen': re['isOpen'], 'isLong': re['isLong']}
        point_list.setdefault(re['Times'], value)


    arr_y = []
    arr_x = []
    position = 0
    count = 0

    position_list = {}

    if 'InnerValue' in InnerValue:
        arr_z = []
        for re in result2[1:]:
            arr_z.append(re[InnerValue['InnerValue']])

    if isLog:
        before_price = result1[0]['LastPrice']
        for re in result1[1:]:
            if count % 20 == 0:
                print(float(re['LastPrice']))
                print(before_price)
                log_d_value = math.log(float(re['LastPrice'])) - math.log(float(before_price))
                print(log_d_value)
                arr_y.append(log_d_value)
                if count % 100 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                value = {'position': position, 'LastPrice': log_d_value}
                position_list.setdefault(time_to_long(re['Times']), value)
                position += 1

            count += 1
            before_price = re['LastPrice']
    else:
        for re in result1:
            print("---------------------------------------")
            print(position)
            if count % 20 == 0:
                arr_y.append(re['LastPrice'])
                if count % 100 == 0:
                    arr_x.append(re['Times'][9:])
                else:
                    arr_x.append("")
                '''
                if position in range(15,20):
                    print("Times Long Position:----------------------------------------")
                    print(re['Times'])
                    print(time_to_long(re['Times']))
                    print(position)
                    if time_to_long(re['Times']) in position_list:
                        print("This time exists")
                '''
                value = {'position': position, 'LastPrice': re['LastPrice']}
                position_list.setdefault(time_to_long(re['Times']), value)
                position += 1
            count += 1

    print("start to plot trend")

    for pos in position_list:
        print(str(pos) + "  ")
        print(position_list[pos])

    if isLog:
        ylabel = "LastPrice_log_d_value"
    else:
        ylabel = "LastPrice"
    print(ylabel)
    '''
    if 'InnerValue' in InnerValue:
        fig, ax1 = plt.subplots(figsize=(60, 35))
        ax2 = ax1.twinx()
        ax2.plot(arr_z, 'yellow')
        ax2.set_label(InnerValue['InnerValue'])
        #title += InnerValue['InnerValue']
    else:
        plt.figure(figsize=(60, 35))
    '''


    arr_point_in_long_x = []
    arr_point_in_long_y = []
    arr_point_in_short_x = []
    arr_point_in_short_y = []
    arr_point_out_long_x = []
    arr_point_out_long_y = []
    arr_point_out_short_x = []
    arr_point_out_short_y = []



    for point in point_list:
        point_list[point]['position'], point_list[point]['LastPrice'] = find_position(point, position_list)

        if point_list[point]['position'] == -1:
            print("Not find position !!")
            break

        data = (point_list[point]['position'], point_list[point]['LastPrice'])
        label = '(' + str(point[9:]) + ', ' + str(point_list[point]['LastPrice']) + ')'
        # crisis_data.append((data,label) )

        if point_list[point]['isOpen'] == 1:
            if point_list[point]['isLong'] == 1:
                arr_point_in_long_x.append(point_list[point]['position'])
                arr_point_in_long_y.append(point_list[point]['LastPrice'])

            else:
                arr_point_in_short_x.append(point_list[point]['position'])
                arr_point_in_short_y.append(point_list[point]['LastPrice'])

        else:
            if point_list[point]['isLong'] == 1:
                arr_point_out_long_x.append(point_list[point]['position'])
                arr_point_out_long_y.append(point_list[point]['LastPrice'])
            else:
                arr_point_out_short_x.append(point_list[point]['position'])
                arr_point_out_short_y.append(point_list[point]['LastPrice'])
            color = "red"
            shift = 0.01
            text = (int(data[0] * 0.9), float(data[1] * (1 - shift)))
            horizontalalignment = 'right'
            verticalalignment = 'bottom'
            # plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
            #         horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)

    iteration = int(60/minute)
    list_point_in_long_x,list_point_in_long_y = get_part_arr_point(arr_point_in_long_y,arr_point_in_long_x,iteration,len(arr_y))
    print(list_point_in_long_x)
    print(list_point_in_long_y)
    list_point_out_long_x, list_point_out_long_y = get_part_arr_point(arr_point_out_long_y, arr_point_out_long_x, iteration,
                                                                    len(arr_y))
    list_point_in_short_x, list_point_in_short_y = get_part_arr_point(arr_point_in_short_y, arr_point_in_short_x, iteration,
                                                                    len(arr_y))
    list_point_out_short_x, list_point_out_short_y = get_part_arr_point(arr_point_out_short_y, arr_point_out_short_x, iteration,
                                                                    len(arr_y))

    for i in range(iteration):
        len_x = len(arr_x)
        print("len_x:" + str(len_x))
        print("len_y:" + str(len(arr_y)))
        start = int(i*(len_x/iteration))
        end = int((i+1)*(len_x/iteration))
        print(start)
        print(end)
        x = arr_x[start:end]
        y = arr_y[start:end]
        title = hour + "_" + Computelantency + "_" + IntervalNum + "_" + MuUpper + "_" + \
                lnLastPriceThreshold + "_" + str(i)
        plt.figure(figsize=(60, 35))

        #plt.ylim(minLastPrice, maxLastPrice)
        plot_sub_figure(x, y, title, "Times", ylabel, False, 3, 35, 45)


        print("finish plot trend and start to plot point")
        point_in_long_y = list_point_in_long_y[i]
        point_out_long_y = list_point_out_long_y[i]
        point_in_short_y = list_point_in_short_y[i]
        point_out_short_y = list_point_out_short_y[i]


        point_in_long_x = list_point_in_long_x[i]
        point_out_long_x = list_point_out_long_x[i]
        point_in_short_x = list_point_in_short_x[i]
        point_out_short_x = list_point_out_short_x[i]

        if len(point_in_long_x) > 0:
            plt.scatter(point_in_long_x, point_in_long_y, c="red", s=1000, marker="*", label="In & Long")
        if len(point_out_long_x) > 0:
            plt.scatter(point_out_long_x, point_out_long_y, c="green", s=1000, marker="o", label="Out & Long")
        if len(point_in_short_x) > 0:
            plt.scatter(point_in_short_x, point_in_short_y, c="pink", s=1000, marker="*", label="In & Short")
        if len(point_out_short_x) > 0:
            plt.scatter(point_out_short_x, point_out_short_y, c="#C1FFC1", s=1000, marker="o", label="Out & Short")
        plt.legend(loc='upper right', prop={'size': 45})

        figname = title + ".png"
        plt.savefig(path + figname, dpi=100)
        plt.close()


        '''
        crisis_data = []
        color = "black"
        shift = 0.005
        text = (int(data[0] * 1.1), float(data[1] * (1 + shift)))
        horizontalalignment = 'left'
        verticalalignment = 'top'
        plt.annotate(label, xy=data, xytext=text, fontsize=45,color=color, arrowprops=dict(facecolor=color, shrink=0.05),
                   horizontalalignment=horizontalalignment, verticalalignment=verticalalignment)
        '''





conn = connect_db('localhost','stockresult','root','0910@mysql')
cur = conn.cursor()
'''
month_list = ["2013", "201303","201304","201305","201306","201307","201308","201309","201310","201311","201312","201405","201411"]


result1 = get_stats_data(cur, "all")
plot_month(result1,0,month_list[1:])

result2 = get_stats_data(cur,"top")
plot_month(result2,1,month_list[1:])


for isTop in range(0,2):
    plot_feature(cur, "ComputeLantency", isTop,month_list)
    plot_feature(cur, "IntervalNum", isTop,month_list)
    plot_feature(cur, "MuUpper", isTop,month_list)
    #plot_feature(cur, "C_I_M", isTop)

'''
'''
trend_month_list = ["201303","201304","201305","201306","201307","201308","201309","201310","201311","201312","201405","201411"]
plot_log_trend(cur,trend_month_list)
'''

#plot_trend(cur)

'''
candidate_day = find_day(cur,"stats_c_i_m","1000")
for day in candidate_day:
    print(day)

    C_I_M = get_C_I_M(candidate_day[day][0])
    print("good")
    print(C_I_M)
    plot_one_day_trade(cur,day,"tradeinfoswithmu",C_I_M[0],C_I_M[1],C_I_M[2],"good")

    C_I_M = get_C_I_M(candidate_day[day][1])
    print("bad")
    print(C_I_M)
    plot_one_day_trade(cur,day,"tradeinfoswithmu",C_I_M[0],C_I_M[1],C_I_M[2],"bad")

    C_I_M = get_C_I_M(candidate_day[day][2])
    print("average")
    print(C_I_M)
    plot_one_day_trade(cur,day,"tradeinfoswithmu",C_I_M[0],C_I_M[1],C_I_M[2],"average")
'''
'''
candidate_day = find_day(cur,"stats_c_i_m","1000")
dest_day = ['20130516','20130625']
isLog = True
for day in candidate_day:
    print(day)
    if day in dest_day:
        C_I_M = get_C_I_M(candidate_day[day][0])
        print("good")
        print(C_I_M)
        plot_one_day_trade_multi(cur, day, "tradeinfoswithmu", C_I_M[0], C_I_M[1], C_I_M[2], "good",isLog)

        C_I_M = get_C_I_M(candidate_day[day][1])
        print("bad")
        print(C_I_M)
        plot_one_day_trade_multi(cur, day, "tradeinfoswithmu", C_I_M[0], C_I_M[1], C_I_M[2], "bad",isLog)

        C_I_M = get_C_I_M(candidate_day[day][2])
        print("average")
        print(C_I_M)
        plot_one_day_trade_multi(cur, day, "tradeinfoswithmu", C_I_M[0], C_I_M[1], C_I_M[2], "average",isLog)

candidate_day = find_day(cur,"stats_c_i_m","1000")
dest_day = ['20130516','20130625']
isLog = False
for day in candidate_day:
    print(day)
    if day in dest_day:
        C_I_M = get_C_I_M(candidate_day[day][0])
        print("good")
        print(C_I_M)
        getInnerValue(cur, day, "innervalue2day", C_I_M[0], C_I_M[1], C_I_M[2], "good",isLog)

        C_I_M = get_C_I_M(candidate_day[day][1])
        print("bad")
        print(C_I_M)
        getInnerValue(cur, day, "innervalue2day", C_I_M[0], C_I_M[1], C_I_M[2], "bad", isLog)

        C_I_M = get_C_I_M(candidate_day[day][2])
        print("average")
        print(C_I_M)
        getInnerValue(cur, day, "innervalue2day", C_I_M[0], C_I_M[1], C_I_M[2], "average", isLog)


day_list = ['20131010', '20131011', '20131016', '20131021', '20131023','20131025','20131029']
plot_feature(cur,"Mu",0,day_list)
plot_feature(cur,"Sigma",0,day_list)
plot_feature(cur,"LossingStopUp",0,day_list)




table_list = ["tradeinfos_InnerValue20130516","tradeinfos_InnerValue20130516fixed","tradeinfos_InnerValue20130516fixed2"]
for table in table_list:
    path = "C:\\Users\\songxue\\Desktop\\stats_info\\" + table + "\\"

    isLog = False
    for i in range(0, 1):
        Mu_path = path + "Mu"
        if not os.path.exists(Mu_path):
            os.makedirs(Mu_path)
        Mu_path += "\\"
        plot_one_day_trade(cur, "20130516",table, "19", "180", "0.001", "unknow", isLog, Mu_path)

        Sigma_path = path + "Sigma"
        if not os.path.exists(Sigma_path):
            os.makedirs(Sigma_path)
        Sigma_path += "\\"
        plot_one_day_trade(cur, "20130516", table, "19", "180", "0.001", "unknow", isLog, Sigma_path)

        LossingStopUp_path = path + "LossingStopUp"
        if not os.path.exists(LossingStopUp_path):
            os.makedirs(LossingStopUp_path)
        LossingStopUp_path += "\\"
        plot_one_day_trade(cur, "20130516", table, "19", "180", "0.001", "unknow", isLog, LossingStopUp_path)
        isLog = True
'''

table_list = ["170807readcsv"]
for table in table_list:

    isLog = False
    #hour_list = ["20130625-09","20130625-10","20130625-11","20130625-13","20130625-14","20130625-15"]
    hour_list = []
    sql = "SELECT distinct mid(Times,1,11) FROM "+ table
    cur.execute(sql)
    result = cur.fetchall()
    for re in result:
        hour_list.append(re['mid(Times,1,11)'])
    print hour_list
    sql = "SELECT distinct ComputeLantency, IntervalNum, InMuUpper, lnLastPriceThreshold FROM " + table
    sql += " where ComputeLantency=6 and IntervalNum=6"
    cur.execute(sql)
    result = cur.fetchall()
    for re in result:
        minitue = 30
        path = "/Users/songxue/Desktop/stock/trade_no_ylim1/" + table + "_" + str(minitue) + "/"
        path += str(re['ComputeLantency']) + "_" + str(re['IntervalNum']) + "_" + str(re['InMuUpper']) + "/"
        if not os.path.exists(path):
            os.makedirs(path)

        for hour in hour_list:
            plot_minute_trade(cur, hour,minitue, table, str(re['ComputeLantency']), str(re['IntervalNum']), str(re['InMuUpper']), str(re['lnLastPriceThreshold']), "unknow", isLog, path)

close_db(cur,conn)



