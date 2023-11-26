from impala.dbapi import connect
from hdbcli import dbapi
import pandas as pd
from dotenv import load_dotenv
import os
import buildTools

# lets load the enviroment
load_dotenv()

# our connection db object
class connectorDB():
    # init our class
    def __init__(self, method, server, uid, pwd, db, port):
        # set the connection variables
        self.method = str(method).upper()
        self.server = server
        self.uid = uid
        self.pwd = pwd
        self.db = db
        self.port = int(port)
        # lets see if the method is valid
        if method not in ('HIVE','HANA'):
            raise Exception(f'Invalid Method {method}')
        
    def connect(self):
        if self.method=='HIVE':
            self.conn = connect(host=self.server, user=self.uid, password=self.pwd, port=self.port, auth_mechanism='PLAIN')
        elif self.method=='HANA':
            self.conn = dbapi.connect(address= self.server, port=self.port, user=self.uid, password=self.pwd)

    def disconnect(self):
        if self.method=='HIVE':
            self.conn.close()
        elif self.method=='HANA':
            self.conn.close()

    def create_df(self,statement):
        if self.method=='HIVE':
            df = pd.read_sql(f"{statement}", self.conn)
        elif self.method=='HANA':
            df = None

        return df
        
    def execute(self, statement):
        if self.method=='HIVE':
            try:
                # create the cursor
                cursor = self.conn.cursor()
                # lets remove teh comments out of the statement before we continue to process it
                newstatement = ""
                for item in statement.split('\n'):
                    if not '--' in item:
                        newstatement = f"{newstatement}{item}\n"
                
                # with hive we cannot execute mutiple statements
                # so we split it and execute it one by one
                executeblocks = newstatement.split(';')
                for executeblock in executeblocks:
                    executeblock = executeblock.strip().rstrip('\n')
                    if len(executeblock)>0:
                        cursor.execute(executeblock)
                self.conn.commit()
            except Exception as e:
                raise e
            finally:
                cursor.close()            
        elif self.method=='HANA':
            try:
                cursor = self.conn.cursor()
                cursor.execute(statement)
            except Exception as e:
                raise e
            finally:
                cursor.close()            
        
    def exec_query(self, statement):
        if self.method=='HIVE':
            try:
                cursor = self.conn.cursor()
                cursor.execute(statement)
                values = cursor.fetchall()
                return values
            except Exception as e:
                raise e
            finally:
                cursor.close()
        elif self.method=='HANA':
            try:
                cursor = self.conn.cursor()
                cursor.execute(statement)
                values = cursor.fetchall()
                return values
            except Exception as e:
                raise e
            finally:
                cursor.close()            
        
    def escape(self, statement):
        return ' '.join(statement.replace("'", "''").split('\n'))
        
# wrapper function to establish the connection       
def establishConnection(method,uid='',pwd=''):
    # lets get the correct env variables for the connection type we want to establish
    if method=="HIVE":
        server=os.environ.get("hive_server")
        if len(str(uid).strip())<=0:
            uid=os.environ.get("hive_uid")
        if len(str(pwd).strip())<=0:
            pwd=buildTools.decodetxt(os.environ.get("hive_pwd"))
        db=os.environ.get("hive_db")
        port=os.environ.get("hive_port")
    elif method=="HANA":
        server=os.environ.get("hana_server")
        if len(str(uid).strip())<=0:
            uid=os.environ.get("hana_uid")
        if len(str(pwd).strip())<=0:            
            pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))
        db=os.environ.get("hana_db")
        port=os.environ.get("hana_port")
    else:
        return {"success":False,"message":f"Method not currently supported {method}"}

    # lets now do the connection    
    try:
        conn = connectorDB(method, server, uid, pwd, db, port)
        conn.connect()
    except Exception as e:
        return {"success":False,"message":str(e).split('\n')[0]}
     
    return {"success":True,"data":conn}

def executeStatement(conn,statement):
    try:
        conn.execute(statement)
    except Exception as e:
        return {"success":False,"message":str(e).split('\n')[0]}
    
    return {"success":True}

def getDF(conn, statement):
    try:
        result = conn.create_df(statement)
    except Exception as e:
        return {"success":False,"message":str(e).split('\n')[0]}
    
    return {"success":True,"data":result}

def getData(conn, statement):
    try:
        result = conn.exec_query(statement)
    except Exception as e:
        return {"success":False,"message":str(e).split('\n')[0]}
    
    return {"success":True,"data":result}

def closeConnection(conn):
    conn.disconnect()
    
def getDBList(method,schema_name):
    # make the connection
    result = establishConnection(method)
    if not result["success"]:
        print(result["message"])
    connection = result["data"]
    
    # lets see how we build the query
    if method=='HANA':
        sql_command =   "SELECT TABLE_NAME,TABLE_TYPE " \
                        "FROM   TABLES " \
                        "WHERE  SCHEMA_NAME = '" + schema_name + "' "
    elif method=='HIVE':
        sql_command =   "SHOW TABLES IN " + schema_name
    # lets get our data
    try:
        dblist = []
        rows = connection.exec_query(sql_command)
        for row in rows:
            dblist.append(row[0])
    except Exception as e:
        raise e
    
    closeConnection(connection)
    
    # return data
    return {"success":True,"data":dblist}
    
        
if __name__ == "__main__":
    method = 'HANA'
    schema_name='SECP1TB_PRD_CL'
    # method = 'HIVE'
    # schema_name='groupbi_gti_isim_dev'
    
    result = getDBList(method,schema_name)
    if result["success"]:
        print(result["data"])
    
    
