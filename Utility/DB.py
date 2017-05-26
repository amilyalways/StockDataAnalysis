import pymysql

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

    # drop table
    def drop_table(self, tablename):
        sql = "drop table if exists " + tablename
        self.cur.execute(sql)
        self.conn.commit()

    # add columns
    def add_columns(self,tablename,column_name,datatype):
        sql = "alter table " + tablename + " add " + column_name +" " + datatype
        self.cur.execute(sql)
        self.conn.commit()

    # drop columns
    def drop_columns(self, tablename, column_name):
        sql = "alter table " + tablename + " drop " + column_name
        self.cur.execute(sql)
        self.conn.commit()



