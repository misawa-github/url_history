#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------------
# Name:
# Purpose:
#
# Author:
#
# Created:     20/06/2019
# Copyright:   (c) phil.liu.20346 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import time
import threading
import apsw

#Note: 1. Database本身不做任何網址重複性檢查
#Note: 2. 上層程式必須要紀錄每頁的最後一筆紀錄的rowid
#Note: 3. Very simple Thread Lock
#Note: 4. No Method Parameters Type check


class UrlHistoryDB(object):


    def __init__(self, dbfile):
        self.connection = apsw.Connection(dbfile)
        self.cursor = self.connection.cursor()
        self.create_table()


    def create_table(self):
        SQL = """BEGIN TRANSACTION;
        CREATE TABLE IF NOT EXISTS URL_History
        (
	       idx INTEGER PRIMARY KEY ASC,
	       title STRING NOT NULL,
        	url STRING NOT NULL,
        	rtime TIMESTAMP,
        	ctime TIMESTAMP,
        	metadata JSON
        );
        COMMIT;"""

        import os
        if os.path.getsize(self.connection.filename) == 0:
            self.lock_execute(SQL)


    def close(self):
        if self.cursor:
            self.cursor.close()

        if self.connection:
            self.connection.close()


    def lock_execute(self, SQL, values=()):
        with threading.Lock():
            self.cursor.execute(SQL, values)


    def add_history(self, title, url, rtime, ctime, metadata):
        SQL = """BEGIN TRANSACTION;
        INSERT INTO URL_History(title, url, rtime, ctime, metadata) VALUES(?,?,?,?,?);
        COMMIT;"""

        if not title:
            title = ""
        if not rtime:
            rtime = ""
        if not ctime:
            ctime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        if not metadata:
            metadata = ""

        values = (title, url, rtime, ctime, metadata)

        self.lock_execute(SQL, values)

        return self.connection.last_insert_rowid()


    def edit_history(self, idx, title, url, rtime, ctime, metadata):
        SQL = """BEGIN TRANSACTION;
        UPDATE URL_History SET title=?, url=?, rtime=?, ctime=?, metadata=? WHERE idx = ?;
        COMMIT;"""

        values = (title, url, rtime, ctime, metadata, idx)

        self.lock_execute(SQL, values)

        return self.connection.changes()


    def delete_history(self, idx):
        SQL = """BEGIN TRANSACTION;
        DELETE FROM URL_History WHERE idx = ?;
        COMMIT;"""

        self.lock_execute(SQL, (idx, ))

        return self.connection.changes()


    def get_page(self, last_rowid=1, item_per_page=100):
        # 上層程式必須要紀錄每頁的最後一筆紀錄的rowid
        SQL = "SELECT idx, title, url, rtime, ctime, metadata FROM URL_History WHERE idx > ? ORDER BY idx LIMIT ?;"
        values = (last_rowid, item_per_page)

        self.lock_execute(SQL, values)

        return self.cursor.fetchall()


    def get_history_count(self):
        SQL = "SELECT count(idx) FROM URL_History;"

        self.cursor.execute(SQL, ())

        return self.cursor.fetchone()[0]


    def export_JSON(self, export_filename):
        SQL = "SELECT idx, title, url, rtime, ctime, metadata FROM URL_History;"

        self.cursor.execute(SQL, ())

        results = self.cursor.fetchall()

        import json

        json.dump(results, open(export_filename, "w") )


    def clear_history_with_expired_days(self, over_days):
        SQL = """BEGIN TRANSACTION;
        DELETE FROM URL_History WHERE ctime <= {0};
        COMMIT;
        """
        expired_day = 'datetime("now", "-{0} days", "localtime")'.format(over_days)

        SQL = SQL.format(expired_day)

        self.lock_execute(SQL)

        return self.connection.changes()





def test():
    dbfile = r"d:\test1.sqlite3"

    db = UrlHistoryDB(dbfile)

    db.add_history("Google", "http://www.google.com.tw", "", "", "")
    #print(db.delete_history(1))
    #print(db.get_page(2, 10))
    #print(db.get_history_count())
    #print(db.edit_history(2, "Yahoo", "http://www.yahoo.com.tw", "", "", ""))
    #db.export_JSON(r"d:\test.json")
    #db.clear_history_with_time_limit(90)
    db.clear_history_with_expired_days(30)
    print(db.get_history_count())
    db.close()



if __name__ == '__main__':
    test()
