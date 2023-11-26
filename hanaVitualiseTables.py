import json
import time
from datetime import datetime
import common_dblib as connector
from dotenv import load_dotenv
import os
import buildTools

from services.hana_rest import HanaRest
from services.helper_functions import HelperFunctions

# lets load the enviroment
load_dotenv()

# the hana object to create dp views
class HanaVirtualiseTables():

    time_str = datetime.now().strftime("%Y%m%d %H:%M:%S")
    new_change_id = None

    cnt_success = 0
    cnt_error = 0
    cnt_new_view = 0
    cnt_existing_view = 0

    cnt_success_tot = 0
    cnt_error_tot = 0
    cnt_new_view_tot = 0
    cnt_existing_view_tot = 0

    dict_view_results = {}
    dict_change_ids = {}

    def __init__(
            self,
            base_url,
            username,
            password,
            source_database,
            target_schema,
            table_name_like='%',
            table_name_not_like='', 
            ignore_raw_tables=True, 
            ignore_history_tables=True, 
            ignore_standard_tables=True): 
        
        # set our values
        self.base_url = base_url
        self.username = username
        self.password = password
        self.source_database = source_database
        self.target_schema = target_schema
        self.table_name_like = table_name_like
        self.table_name_not_like = table_name_not_like
        self.ignore_raw_tables = ignore_raw_tables
        self.ignore_history_tables = ignore_history_tables
        self.ignore_standard_tables = ignore_standard_tables

    def virtualise_tables(self):
        # lets establish a connection by creating our connection class
        result = connector.establishConnection('HANA')
        if not result["success"]:
            return result
        self.hdb = result["data"]

        self.hr = HanaRest(self.base_url, self.username, self.password)

        # Instantiate our helper function
        self.hf = HelperFunctions(self.hdb)

        # Initialise counters
        self.cnt_success = 0
        self.cnt_error   = 0
        cnt = 0

        # Get the list of tables to virtualise
        virtual_tables = self.hf.get_virtual_table_names(            
            source_database = self.source_database, 
            target_schema = self.target_schema,  
            table_name_like = self.table_name_like, 
            table_name_not_like = self.table_name_not_like, 
            ignore_raw_tables = self.ignore_raw_tables, 
            ignore_history_tables = self.ignore_history_tables,
            ignore_standard_tables = self.ignore_standard_tables)
        
        print()

        for row in virtual_tables:
            cnt += 1
            source_table = row['SOURCE_TABLE']
            target_table = row['TARGET_TABLE']
            table_count  = row['TABLE_COUNT']

            # If the virtual table already exists then simply refresh it. If not then try to provision it
            if table_count == 0:
                ret_val = self.hf.try_provision_virtual_table(source_table, target_table)
            else:
                ret_val = self.hf.try_refresh_virtual_table(target_table)

            # Keep count of success and errors
            if ret_val == True:
                self.cnt_success += 1
            else:
                self.cnt_error += 1   
            
            # Show count
            if cnt % 10 == 0:
                print("X" if ret_val == True else "?", end="", flush=True)
            else:
                print("x" if ret_val == True else "?", end="", flush=True)     
                
        #Check the overall status
        if self.cnt_success > 0 and self.cnt_error == 0:
            result = "Success"
            result_code = 0
        elif self.cnt_success > 0 and self.cnt_error > 0:
            result = "Success with errors"
            result_code = 1
        elif self.cnt_success == 0 and self.cnt_error > 0:
            result = "ERROR"
            result_code = 2
        elif self.cnt_success == 0 and self.cnt_error == 0:
            result = "Success (nothing processed)"
            result_code = 0

        tmp_dict_results = {
            "result": result,
            "result_code" : result_code,
            "cnt_success" : self.cnt_success,
            "cnt_error" : self.cnt_error
        }

        print(end="\n")
        print(end="\n")
        print(json.dumps(tmp_dict_results, indent=2))
        
        return {"success":True,"data":tmp_dict_results}

def virtualiseTables(
        source_database,
        target_schema,
        table_name_like='%',
        table_name_not_like='',
        ignore_raw_tables=True,
        ignore_history_tables=True, 
        ignore_standard_tables=True):
    
    # lets gets some env variables
    base_url=os.environ.get("hana_base_url")
    uid=os.environ.get("hana_uid")
    pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))
    
    # create our virtual tables
    hvt = HanaVirtualiseTables(        
            base_url = base_url,
            username = uid,
            password = pwd,
            source_database = source_database,
            target_schema = target_schema,
            table_name_like = table_name_like,
            table_name_not_like = table_name_not_like, 
            ignore_raw_tables = ignore_raw_tables, 
            ignore_history_tables = ignore_history_tables,
            ignore_standard_tables=ignore_standard_tables)
    
    result = hvt.virtualise_tables()
    if not result["success"]:
        return result
    
    return {"success":True}

if __name__ == "__main__":
    
    #virtualiseTables('groupbi_secp1tb_prd','RON_TEST_1_CL', '%')
    result = virtualiseTables('groupbi_ops_gbiopslab','DIETER_TEST_1_CL', 'tcurrent%')
    if result["success"]:
        print("DONE")
    else:
        print(result["message"])
    
    