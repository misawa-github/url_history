#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:
#
# Created:     04/07/2019
# Copyright:   (c) phil.liu.20346 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------


import bottle
import url_history_db

Database_File = "test.sqlite3"
_DB = url_history_db.UrlHistoryDB(Database_File)

API_Action_Map = {
    "add": _DB.add_history,
    "edit": _DB.edit_history,
    "delete": _DB.delete_history,
    "count": _DB.get_history_count,
}


@bottle.route("/urlhistory/api/<action>", method="post")
def execute_api_action(action):
    data = bottle.request.json

    if action not in API_Action_Map:        
        return {"result":"No match action found"}
    
    if data:
        return {"result":API_Action_Map[action](**data)}
    else:
        return {"result":API_Action_Map[action]()}

    




def main():
    bottle.debug(True)
    bottle.run(reloader=True)

if __name__ == '__main__':
    main()
