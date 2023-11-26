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
class GenHanaDpViews():

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
            source_schema,
            target_package_name,
            table_name_like,
            table_names_in = None,
            jira_number = 'TEST'):
        
        # set our values
        self.base_url = base_url
        self.username = username
        self.password = password
        self.source_schema = source_schema
        self.target_package_name = target_package_name
        self.jira_number = jira_number
        self.table_name_like = table_name_like
        self.table_names_in = table_names_in
        
    def get_new_change_id(self):
        # Check if we've already created a new ChangeId
        if not self.new_change_id:
            #Create the change list
            if self.jira_number is not None:
                desc = f"AutoGen DP Views - {self.jira_number} - {self.source_schema} - {self.target_package_name} - {self.time_str}"
            else:
                desc = f"AutoGen Roles - {self.source_schema} - {self.target_package_name} - {self.time_str}"

            response = self.hr.create_change(desc)
            if response.status_code == 200:
                # Get the ChangeId
                response_json = json.loads(response.text)
                self.new_change_id = response_json['ChangeId']
                return self.new_change_id
        else:
            return self.new_change_id
        

    def generate_dp_views(self):
        # lets establish a connection by creating our connection class
        result = connector.establishConnection('HANA')
        if not result["success"]:
            return result
        self.hdb = result["data"]

        self.hr = HanaRest(self.base_url, self.username, self.password)

        # Instantiate our helper function
        self.hf = HelperFunctions(self.hdb)

        # We no longer process the virtualisation stuff here, done in diff package (TBA)
        # Create the ddl package and schema
        #response = hr.create_package(parent_ddl_package, target_ddl_package)

        # Check that the source schema has virtual tables. Also check if this is a CDC source
        table_cnt, cdc_cnt = self.hf.get_cdc_count(self.source_schema)
        if table_cnt <= 0:
            #Nothing to process, exit with message
            self.quit_with_message(None, f"No tables found for source schema {self.source_schema}")


        # Create the views
        if cdc_cnt == 0:
            result = self.create_views(self.target_package_name, None)

            # Add the result to the outer result dict
            self.dict_view_results["base"] = result

        elif cdc_cnt > 0:

            for cdc_type in ["base","current","full","currslm"]:
                cdc_target_calc_view_package = self.target_package_name + "." + cdc_type
                result = self.create_views(cdc_target_calc_view_package, cdc_type)

                if len(result) > 0:
                    # Add the result to the outer result dict
                    self.dict_view_results[cdc_type] = result

                    # Add counts to total
                    self.cnt_success_tot += self.cnt_success
                    self.cnt_error_tot += self.cnt_error
                    self.cnt_new_view_tot += self.cnt_new_view
                    self.cnt_existing_view_tot += self.cnt_existing_view

            # Update totals after we've processed the various CDC types
            self.dict_view_results["success_tot"] = self.cnt_success_tot
            self.dict_view_results["error_tot"] = self.cnt_error_tot
            self.dict_view_results["new_views_tot"] = self.cnt_new_view_tot
            self.dict_view_results["existing_views_tot"] = self.cnt_existing_view_tot
            self.dict_view_results["change_ids"] = ",".join(list(self.dict_change_ids.keys()))

        print()
        print(json.dumps(self.dict_view_results, indent=2))
        
        return {"success":True,"data":self.dict_view_results}

    def create_views(self, target_calc_view_package, cdc_type):

        # Resent counts to 0
        self.cnt_success = 0
        self.cnt_error = 0
        self.cnt_new_view = 0
        self.cnt_existing_view = 0
        

        cnt = 0

        tmp_dict_results = {}

        # Get the Calc Views
        calc_views = self.hf.get_calc_views(self.source_schema, self.table_name_like, cdc_type)

        # Make sure something was returned
        if len(calc_views) > 0:

            #Check/create the target calc view package 
            package_exists, dp_view_pkg_status = self.hr.check_if_package_exists(target_calc_view_package)

            #Check that the package was created successfully (or exists). 
            if package_exists == True:
                
                for row in calc_views:
                    cnt += 1

                    calc_view_name = row["TABLE_NAME"] + ".calculationview"
                    calc_view_xml = row["CALC_VIEW_SOURCE"]

                    # Check if the view is in the list to be processed (if a list was provided)
                    if self.table_names_in is not None:
                        if  row["TABLE_NAME"] not in self.table_names_in:
                            # A list of tables to be processed has been specified and the current table name
                            # is not in the list. Simply skip this item in the for loop
                            continue;


                    fq_file_name = target_calc_view_package.replace('.', '/') + '/' + calc_view_name

                    # Check if this view is part of an existing change
                    response = self.hr.get_changes(fq_file_name)

                    if response.status_code == 200:
                        response_json = json.loads(response.text)
                        
                        if response_json['ChangeId']:
                            change_id = response_json['ChangeId']
                            self.cnt_existing_view += 1
    
                        else:     
                            #Create the change list
                            change_id = self.get_new_change_id()
                            self.cnt_new_view += 1

                        # Add the ChangeId to the list of Change IDs 
                        self.dict_change_ids.setdefault(change_id)

                        # Create the Calc view
                        response = self.hr.write_file(target_calc_view_package, calc_view_name,calc_view_xml, change_id)
                        
                        if response.status_code in (200, 202):
                            self.cnt_success += 1
                        else:
                            self.cnt_error += 1
            
                        # Show count
                        if cnt % 10 == 0:
                            print("X" if response.status_code in (200, 202) else "?", end="", flush=True)
                        else:
                            print("x" if response.status_code in (200, 202) else "?", end="", flush=True)     

                print("", end = "\r\n")
                
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

            else:
                result = "ERROR"
                result_code = 2

            tmp_dict_results = {
                "result": result,
                "result_code" : result_code,
                "dp_view_package" : dp_view_pkg_status,
                "cnt_success" : self.cnt_success,
                "cnt_error" : self.cnt_error
            }

        return tmp_dict_results


    def quit_with_message(self, change_id, message):
        self.dict_view_results = {
            "result": "ERROR",
            "result_code" : 2,
            "message" : message,
            "cnt_success" : self.cnt_success,
            "cnt_error" : self.cnt_error,
            "change_id": change_id
        }

        print()
        print(json.dumps(self.dict_view_results, indent=2))

        quit()

def generateDPViews(source_schema,target_package_name,table_name_like='%'):
    # lets gets some env variables
    base_url=os.environ.get("hana_base_url")
    uid=os.environ.get("hana_uid")
    pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))

    # create our hana dp view object
    ghdv = GenHanaDpViews(base_url,uid,pwd,source_schema,target_package_name,table_name_like)
    result = ghdv.generate_dp_views()
    if not result["success"]:
        return result
    
    return {"success":True}

def generateDPViewsFrom(source_schema,target_package_name,blacklist,whitelist,ignorelist=''):
    # lets gets some env variables
    base_url=os.environ.get("hana_base_url")
    uid=os.environ.get("hana_uid")
    pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))
    
    # lets get all the objects for that schema
    result = connector.getDBList("HANA",source_schema)
    if not result["success"]:
        return result
    tablelist = result["data"]
    # lets check that we have data at leats no need to continue if you have nothing
    if len(tablelist)<=0:
        return {"success":False,"message":f"No tables found for {source_schema}"}

    # lets read the files in next and converted the values to list items
    # blacklist
    bl = []
    result = buildTools.loadInputFiletoList(blacklist)
    if result["success"]:
        bl = result["data"]
    # whitelist
    wl = []
    result = buildTools.loadInputFiletoList(whitelist)
    if result["success"]:
        wl = result["data"]
    # whitelist
    il = []
    result = buildTools.loadInputFiletoList(ignorelist)
    if result["success"]:
        il = result["data"]
        
    # now lets see which tables we can actually use or not
    result = buildTools.buildUseList(tablelist,wl,bl,il)
    if not result["success"]:
        return result
    table_names_in = result["data"]
        
    # create our hana dp view object from our list
    print(f"Processing DP Views......")
    table_name_like = '%'
    ghdv = GenHanaDpViews(base_url,uid,pwd,source_schema,target_package_name,table_name_like, table_names_in)
    ghdv.generate_dp_views()  
    
    return {"success":True}

if __name__ == "__main__":
    #result = generateDPViews('SECP1TB_PRD_CL',f'bi.sandbox.{os.getlogin().upper()}.dev.dp.SECP1TB')
    result = generateDPViews('DIETER_TEST_1_CL',f'bi.sandbox.{os.getlogin().upper()}.dev.dp.DIETER_TEST_1_CL','tcurrent%')
    if result["success"]:
        print("DONE")
    else:
        print(result["message"])
    