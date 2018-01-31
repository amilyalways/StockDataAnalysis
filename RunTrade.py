# -*- coding: utf-8 -*-

import commands
import collections
import time
from Data.ImExport import ImExport
from Utility.DB import DB
from Data.StatsRevenue import StatsRevenue
import math
import numpy as np
import pandas as pd
import os




class RunTrade:
    allParas = collections.OrderedDict()
    paras_names = []
    allParas_means_stds = collections.OrderedDict()
    paras_tableName = ""
    start_id = 0
    minDayTradeNum = 15
    min_dist_convergence = 0.5
    top_paras_num = 5
    neighbor_paras_num = 2





    def __init__(self, fixed_paras, thresholds, weights, dbs, imex):
        self.allParas = fixed_paras['allParas']
        self.paras_names = self.allParas.keys()
        for key in self.allParas:
            means = np.mean(allParas[key])
            std = np.std(allParas[key])
            self.allParas_means_stds.setdefault(key, (means, std))
        self.paras_tableName = paras_tableName
        self.start_id = fixed_paras['start_id']

        self.minDayTradeNum = thresholds['minDayTradeNum']
        self.min_dist_convergence = thresholds['min_dist_convergence']
        self.top_paras_num = thresholds['top_paras_num']
        self.neighbor_paras_num = thresholds['neighbor_paras_num']

        self.weights = weights

        self.db = dbs['db']
        self.db_remote = dbs['db_remote']

        self.imex = imex

    # 对参数向量进行归一化处理
    def normalization_paras(self, paras):
        for key in self.allParas_means_stds:
            mean = self.allParas_means_stds[key][0]
            std = self.allParas_means_stds[key][1]
            paras[key] = (paras[key] - mean)/std

        return paras

    #判断两个参数向量的相似度是否在阈值以内
    def is_two_paras_similar(self, paras1, paras2):
        dist = 0
        num = len(paras1.keys())
        for key in paras1:
            dist += math.pow((paras1[key]-paras2[key]), 2)
        dist /= num
        print "dist: ", dist

        if dist <= self.min_dist_convergence:
            return True
        else:
            return False

    #判断现在测试的参数是否收敛
    def is_paras_converge(self, paras):
        sql_num = "select count(*) from " + self.paras_tableName
        self.db_remote.cur.execute(sql_num)
        end_id = self.db_remote.cur.fetchall()[0]['count(*)']
        print "end_id: ", end_id

        sql_paras = "select * from " + self.paras_tableName + " limit " + str(self.start_id) \
                    + ", " + str(end_id-self.start_id)
        df_runned_paras = pd.read_sql(sql_paras, self.db_remote.conn)

        paras = self.normalization_paras(paras)

        for row in df_runned_paras.index:
            print "row_id", row
            runned_paras = {}
            for key in self.paras_names:
                runned_paras.setdefault(key, df_runned_paras.get_value(row, key))
            runned_paras = self.normalization_paras(runned_paras)
            print "normalization runned paras: "
            print runned_paras
            if self.is_two_paras_similar(runned_paras, paras):
                return True

        return False

    #计算参数的稳定性和盈利表现的得分
    def calculate_stable_benfit_score(self, df_paras):
        #df_paras['benfit_score'] = df_paras['total_revenue'] / df_paras['total_revenue'].max()
        df_paras['benfit_score'] = (df_paras['total_revenue'] - df_paras['total_revenue'].mean())/df_paras['total_revenue'].std()

        paras_len = df_paras.shape[0]
        print "paras_len: ", paras_len
        for row in df_paras.index:
            neighbors = []
            print "row: ", row
            if row < self.neighbor_paras_num / 2:
                left = row
                right = self.neighbor_paras_num - left + 1
            elif row >= paras_len - self.neighbor_paras_num / 2:
                right = paras_len - row - 1
                left = self.neighbor_paras_num - right + 1
            else:
                left = self.neighbor_paras_num/2 + 1
                right = self.neighbor_paras_num/2 + 1

            for i in range(1, left):
                neighbors.append(df_paras.get_value(row-i, 'total_revenue'))
            print "left: ", left
            print "right: ", right
            neighbors.append(df_paras.get_value(row, 'total_revenue'))

            for i in range(1, right):
                neighbors.append(df_paras.get_value(row+i, 'total_revenue'))
            stable_score = np.std(neighbors)

            df_paras.set_value(index=row, col='stable_score', value=stable_score)

        df_paras['stable_score'] = df_paras['stable_score'] / df_paras['stable_score'].max()
        df_paras['stable_score'] = 1 - df_paras['stable_score']

        df_paras['score'] = map(lambda x, y: self.calculate_score(x, y), df_paras['benfit_score'], df_paras['stable_score'])

        return df_paras


    #计算参数的得分,综合考虑了稳定性和盈利表现
    def calculate_score(self, benfit_score, stable_score):
        a = (self.weights['stable_weight'] + self.weights['benfit_weight']) * stable_score * benfit_score
        b = (self.weights['stable_weight']*benfit_score + self.weights['benfit_weight']*stable_score)
        if b != 0:
            score = a/b
        else:
            score = 0
        return score

    #找到最适合的几组参数,进行下一次迭代
    def pick_good_paras(self, trade_tableName, date, stats):
        revenue_tableName = "revenue" + trade_tableName[10:]
        print revenue_tableName
        #self.save_revenue_mysql(imex_remote, trade_tableName, 100000, revenue_tableName, "", False, [])

        condition = ["ComputeLantency", "IntervalNum", "MuUpper", "MuLower", "lnLastPriceThreshold",
                     "A"]
        df = stats.stats_revenue(revenue_tableName, condition, 'Revenue')

        print "************"
        df = self.calculate_stable_benfit_score(df)
        df_good_candicate = df[df['avgDayTradeNum'] >= self.minDayTradeNum]
        df_good = df_good_candicate.sort_values(by='score', ascending=False)
        print "df_good: -------------------------"
        print df_good[:5]

        path = "/home/emily/桌面/stockResult/stats" + str(date) + "/"
        if not os.path.exists:
            os.makedirs(path)
        imex.save_df_csv(df, path, "stats_" + revenue_tableName + ".csv")

    def addNewParas_mysql(self, db, newParas, trade_start_day, trade_end_day, parastableName, destTableName):
        sql_num = "select count(*) from " + parastableName
        db.cur.execute(sql_num)
        new_id = db.cur.fetchall()[0]['count(*)']
        print new_id
        startId = new_id + 1
        endId = new_id + len(new_paras)
        destTableName += "_" + str(startId) + "_" + str(endId)
        for rowParas in newParas:
            new_id += 1
            values = str(new_id)
            for paras in rowParas:
                values += ", " + str(paras)
            values += ", '" + destTableName + "'" + ", " + trade_start_day + ", " + trade_end_day + ", 0"
            db.insert(parastableName, values)


        return startId, endId, destTableName

    def isFinishTrade(self, db, startId, endId, parastableName):
        sql = "select min(isFinished) from " + parastableName + \
              " where paras_id  between " + str(startId) + " and " + str(endId)
        db.cur.execute(sql)
        rs = db.cur.fetchall()[0]['min(isFinished)']
        print sql
        print rs
        if rs == 1:
            return True
        else:
            return False

    def iterationTrade_FindGoodParas(self, new_paras, trade_start_day, trade_end_day, paras_tableName):
        is_stable = False
        date = time.strftime('%Y%m%d', time.localtime(time.time()))

        while not is_stable:
            for key in self.paras:
                trade_tableName = "tradeinfos" + s + "_" + key

                startId, endId, trade_tableName = self.addNewParas_mysql(
                    db_remote, new_paras, trade_start_day, trade_end_day, paras_tableName, trade_tableName)
                print "startId: " + str(startId)
                print "endId: " + str(endId)

                while True:
                    isFinishTrade = self.isFinishTrade(db_remote, startId, endId, paras_tableName)

                    if isFinishTrade:
                        is_stable, new_paras = stats.pick_good_paras(trade_tableName, date)
                        break
                    else:
                        print "Trade is not finished... Please wait..."
                        time.sleep(5)
                        sql = "update " + paras_tableName + \
                              " set isFinished=1"
                        db_remote.cur.execute(sql)
                        db_remote.conn.commit()

        return new_paras


if __name__ == '__main__':

    allParas = collections.OrderedDict()
    allParas['ComputeLantency'] = [6, 8]
    allParas['IntervalNum'] = [10, 200]
    allParas['MuUpper'] = [1, 2, 3]
    allParas['MuLower'] = [0.5, 0.6]
    allParas['lnLastPriceThreshold'] = [0.003, 0.002]
    allParas['A'] = [0.001, 0.002]

    paras_tableName = "paras_need_run"
    fixed_paras = {
        'allParas': allParas,
        'paras_tableName': paras_tableName,
        'start_id': 0
    }

    thresholds = {
        'minDayTradeNum': 15,
        'min_dist_convergence': 0.5,
        'top_paras_num': 5,
        'neighbor_paras_num': 2
    }

    weights = {
        'stable_weight': 0,
        'benfit_weight': 1
    }

    dbs = {
        'db': DB('localhost', 'stockresult', 'root', '0910@mysql'),
        'db_remote': DB("10.141.221.124", "stockresult", "root", "cslab123")
    }
    imex = ImExport(dbs['db'])


    run_trade = RunTrade(fixed_paras, thresholds, weights, dbs, imex)

    initialParas = {
        'ComputeLantency': 20,
        'IntervalNum': 540,
        'MuUpper': 0.01,
        'MuLower': 0.02,
        'lnLastPriceThreshold': 0.003,
        'A': 0.1
    }


    stats = StatsRevenue()
    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    run_trade.pick_good_paras("tradeinfos20180122_c", date, stats)

'''

    initialParas = {
        'ComputeLantency': [6,8],
        'IntervalNum': [10,200],
        'MuUpper': [],
        'MuLower': [],
        'lnLastPriceThreshold': [],
        'A': []
    }
    paras = initialParas

    is_converge = run_trade.is_paras_converge(initialParas)
    if is_converge:
        print "paras converge"
    else:
        print "paras not converge"

    stats = StatsRevenue()



    new_paras = [[6, 840, 0.01, 0.02, 0.003, 0.1], [20, 240, 0.01, 0.02, 0.003, 0.1]]
    trade_start_day = "201303"
    trade_end_day = "201307"
    stable_paras = run_trade.iterationTrade_FindGoodParas(new_paras, trade_start_day, trade_end_day, paras_tableName)

    date = time.strftime('%Y%m%d', time.localtime(time.time()))
    stable_trade_tables = "tradeinfos" + date + "_stable"
    trade_start_day = "201308"
    trade_end_day = "201312"
    allParas = {
        'ComputeLantency': [6, 8],
        'IntervalNum': [10, 200],
        'MuUpper': [1, 2, 3],
        'MuLower': [0.5, 0.6],
        'lnLastPriceThreshold': [0.003, 0.002],
        'A': [0.001, 0.002]
    }
    run_trade.addNewParas_mysql(db_remote, stable_paras, trade_start_day, trade_end_day, paras_tableName, stable_paras)
'''


   



















