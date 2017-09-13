# -*- coding: utf-8 -*-
import pymysql
import pandas as pd

class DB:


    # connect to db
    def __init__(self,host,db,username,password):
        config = {
            'host': host,
            'port': 3306,
            'user': username,
            'password': password,
            'db': db,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
        }
        self.conn = pymysql.connect(**config)
        self.cur = self.conn.cursor()

    # close db
    def close(self):
        self.cur.close()
        self.conn.close()

    # select
    def select(self, tablename, columns, **condition):
        sql = "select " + str(columns) + " from " + str(tablename)
        if 'condition' in condition:
            sql += ' where ' + str(condition['condition'])
        if 'limit' in condition:
            sql += " limit " + str(condition['limit']['start']) + ", " + str(condition['limit']['offset'])
        print(sql)
        self.cur.execute(sql)
        result = self.cur.fetchall()
        return result

    # insert
    def insert(self, tablename, values, **condition):
        sql = "insert into "
        if 'columns' in condition:
            sql += "( " + condition['columns'] + " ) "

        sql += tablename + " values( "+ values + " )"
        print(sql)
        self.cur.execute(sql)
        self.conn.commit()

    # delect all rows or some rows
    def delete(self,tablename, **condition):
        sql = "delete from " + tablename
        if 'condition' in condition:
            sql += " where " + condition['condition']
        print(sql)
        self.cur.execute(sql)
        self.conn.commit()

    # update table
    def update(self, tablename, colums, **condition):
        sql = "update " + tablename + "set " + colums
        if 'condition' in condition:
            sql += " where " + condition
        self.cur.execute(sql)
        self.conn.commit()

    # create table
    def create_table(self, tablename, filedlist, *isDrop):
        if isDrop:
            self.drop_table(tablename)
        sql = "create table if not exists " + tablename + "(" + filedlist + ")"
        self.cur.execute(sql)
        self.conn.commit()

    def create_table_copy(self, fromtable, totable, copy_cols=[], add_cols={}, isDrop=False):
        if isDrop:
            self.drop_table(totable)
        sql = "create table if not exists " + totable + " as select "
        if len(copy_cols) > 0:
            for col in copy_cols:
                sql += col + ", "
            sql = sql[:-2]
        else:
            sql += "*"
        sql += " from " + fromtable + " limit 0"
        print sql
        self.cur.execute(sql)
        self.conn.commit()

        if len(add_cols) > 0:
            for col in add_cols:
                self.add_column(totable, col, add_cols[col])

    # drop table
    def drop_table(self, tablename):
        sql = "drop table if exists " + tablename
        self.cur.execute(sql)
        self.conn.commit()

    # add columns
    def add_column(self,tablename,column_name,datatype):
        sql = "alter table " + tablename + " add " + column_name + " " + datatype
        self.cur.execute(sql)
        self.conn.commit()

    # drop columns
    def drop_column(self, tablename, column_name):
        sql = "alter table " + tablename + " drop " + column_name
        self.cur.execute(sql)
        self.conn.commit()

    # modify columns name
    def rename_column(self, tablename, old_name, new_name, datatype):
        sql = "alter table " + tablename + " change column " + old_name \
              + " " + new_name + " " + datatype
        self.cur.execute(sql)
        self.conn.commit()

    # modify columns order
    def reorder_column(self, tablename, col_name, datatype, pre_col):
        sql = "alter table " + tablename + " change column " + col_name \
              + " " + col_name + " " + datatype + " default null after " + pre_col
        self.cur.execute(sql)
        self.conn.commit()

if __name__ == '__main__':
    db = DB('localhost', 'stockresult','root','0910@mysql')
    add_cols = {'Revenue': "double"}
    from_tables = ["tradeinfos20170913", "tradeinfos_cut20170913", "tradeinfos_anti20170913"]
    to_tables = ["revenue20170913", "revenue_cut20170913", "revenue_anti20170913"]
    tables = zip(from_tables, to_tables)
    for (from_table, to_table) in tables:
        print from_table, to_table
        db.create_table_copy(from_table, to_table, add_cols=add_cols)
    #db.create_table_copy("170807readcsv", "tradeinfos20170911" )

    '''
    df = pd.read_csv("/home/emily/桌面/history_with_time2.csv")
    df = df.values


    record_num = len(df)
    print record_num

    for record in df:
        print record
        values = "'" + record[0] + "',"
        for word in record[1:]:
            values += str(word) + ","
        values = values[:-1]
        db.insert("tradeinfos20170911", values)
    '''






