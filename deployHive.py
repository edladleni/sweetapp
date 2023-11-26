import buildTools
import common_dblib as connector
import os
from sql_metadata import Parser

def validateDB(statement, conn):
    # first things first lets get all the databases
    getdbstatement = "show databases"
    result = connector.getData(conn,getdbstatement)
    if not result["success"]:
        return result
    values = result["data"]
    
    # lets strip the comments out of the statement
    newstatement = ""
    for item in statement.split('\n'):
        if not '--' in item:
            newstatement = f"{newstatement}{item}\n"
    
    # next we parse the database name from the statement
    db = ''
    for item in newstatement.split(';'):
        # we only check items that have a proper length
        if len(item.strip())>0:
            if  'create external' in (item.lower()):
                pass
            elif 'create' in (item.lower()) or 'drop' in (item.lower()):
                parser = Parser(item).tables
                # lets get the db do some checks
                if '.' in parser[0]:
                    dbcheck = parser[0].split('.')[0].lower()
                else:
                    dbcheck = parser[0].lower()
                # lets see if we have the value for the db set 
                if len(db)==0:
                    db = dbcheck
                # next we check that the values are the same
                if db!=dbcheck:
                    return {"success":False,"message":"Multiple Databases set in the script"}
                    
    # now we see if we can find our database
    found = False
    for item in values:
        if item[0].lower()==db:
            found = True
            break

    # return our result
    if not found:
        return {"success":False,"message":f"DB not created {db}"}
    
    return {"success":True,"data":db}

def deployStatement(statement,conn=None):
    # create our connection
    if not conn:
        result = connector.establishConnection('HIVE')
        if not result["success"]:
            return result
        conn = result["data"]
        
    # lets now execute our statement
    result = connector.executeStatement(conn,statement)
    if not result["success"]:
        return result

    return {"success":True,"message":"Statemnet Executed"}

def deployScript(scriptlist,archivefolder,conn=None,action='archive'):
    # create our connection
    if not conn:
        result = connector.establishConnection('HIVE')
        if not result["success"]:
            return result
        conn = result["data"]
    # lets see how we received the scripts to execute 
    # either as a string or a list of strings containing the script name
    if isinstance(scriptlist,str):
        result = buildTools.loadInputFiletoString(scriptlist)
        if not result["success"]:
            return result
        datastring = result["data"]
        result = deployStatement(datastring,conn)
        if not result["success"]:
            return result
    elif isinstance(scriptlist,list) or isinstance(scriptlist,tuple):
        for path in scriptlist:
            if len(path.strip())>0:
                path = path.replace('\\','/')
                print(f"Processing {path}")
                # load the sscript into a string value
                result = buildTools.loadInputFiletoString(path)
                if not result["success"]:
                    return result
                # get the statement
                datastring = result["data"]
                # validate the database within the statement
                result = validateDB(datastring,conn)
                if not result["success"]:
                    return result
                # execute the statement
                result = deployStatement(datastring,conn)
                if not result["success"]:
                    return result
                # move the successfull file to either to be deleted/archived or kept
                if action == 'archive':
                    archivefolder = archivefolder.replace('\\','/')
                    print(f"Archiving {path} in {archivefolder}")
                    buildTools.archiveFile(path,archivefolder)
                elif action == 'delete':
                    print(f"Deleteing {path}")
                    buildTools.deleteFile(path)
                
    else:
        return {"success":False,"message":f"Data Type {type(scriptlist)} Not Support"}
    
    return {"success":True,"message":"Script(s) executed"}

def deployFolder(folder='',ext='hql',archivefolder='',conn=None, action='archive',processraw=True):
    # if no folder is provided default to the current folder
    if len(folder)<=0:
        folder = os.getcwd()
    # create our connection
    if not conn:
        result = connector.establishConnection('HIVE')
        if not result["success"]:
            return result
        conn = result["data"]
    # get all the scripts in the folder
    for path in os.listdir(folder):
        # we only deploy scripts that has the passed extension
        # we also see if we have a main or raw file only for hive - with raw we will see if we must ignore it or not
        if f".{ext}" in path and ("main_" in path or ("raw_" in path and processraw)):
            filetoprocess = f"{folder}/{path}".replace('\\','/')
            print(f"Processing {filetoprocess}")

            result = buildTools.loadInputFiletoString(filetoprocess)
            if not result["success"]:
                return result
            # get the statement
            datastring = result["data"]
            # validate the database within the statement
            result = validateDB(datastring,conn)
            if not result["success"]:
                return result
            # execute the statement
            result = deployStatement(datastring,conn)
            if not result["success"]:
                return result
            # move the successfull file to either to be deleted/archived or kept
            if action == 'archive':
                archivefolder = archivefolder.replace('\\','/')
                print(f"Archiving {filetoprocess} in {archivefolder}")
                buildTools.archiveFile(filetoprocess,archivefolder)
            elif action == 'delete':
                print(f"Deleteing {filetoprocess}")
                buildTools.deleteFile(filetoprocess)

    return {"success":True,"message":"Script(s) executed"}

if __name__ == "__main__":
    # result = deployScript(["c:\\Dev\\DEA_Tools\\inputs\\test.hql",])
    # if not result["success"]:
    #     print(result["message"])
    # else:
    #     print("Script executed")
        
    # result = deployFolder("c:\\Dev\\DEA_Tools\\inputs\\","hql")
    # if not result["success"]:
    #     print(result["message"])
    # else:
    #     print("Script executed")
        
    # result = connector.establishConnection('HIVE')
    # if not result["success"]:
    #     print(result["message"])
    # else:
    #     conn = result["data"]
    #     statement = """
    #     set hive.exec.dynamic.partition=true; 
    #     set hive.exec.dynamic.partition.mode=nonstrict;
        
    #     drop table if exists groupbi_ops_gbiopslab.platform_fees_abc;
        
    #     create table groupbi_ops_gbiopslab.platform_fees_abc (
    #         load_dts varchar(80),
    #         filename varchar(80),
    #         product_portfolios varchar(100),
    #         suf_alloc_jan2018 decimal(10,6)
            
    #     );        
    #     """
    #     result = validateDB(statement, conn)
    #     if not result["success"]:
    #         print(result["message"])
    #     else:
    #         print(result["data"])
    
    pass
 