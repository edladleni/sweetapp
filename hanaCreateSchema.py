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
class HanaCreateSchema():

    time_str = datetime.now().strftime("%Y%m%d %H:%M:%S")
    new_change_id = None

    dict_schema_results = {}
    dict_change_ids = {}

    def __init__(self, base_url, username, password, schema_name, jira_number="GBO-000001", base_package="bi.ddl"): 
        # set our values
        self.base_url = base_url
        self.username = username
        self.password = password
        self.schema_name = schema_name
        self.base_package = base_package
        self.jira_number = jira_number

        self.target_package_name = base_package + "." + schema_name + ".schema"
        self.target_schema_file_name = schema_name + ".hdbschema"
        
    def get_new_change_id(self):
        # Check if we've already created a new ChangeId
        if not self.new_change_id:
            #Create the change list
            if self.jira_number is not None:
                desc = f"Create New Schema - {self.jira_number} - {self.schema_name} - {self.target_package_name} - {self.time_str}"
            else:
                desc = f"Create New Schema - {self.schema_name} - {self.target_package_name} - {self.time_str}"

            response = self.hr.create_change(desc)
            if response.status_code == 200:
                # Get the ChangeId
                response_json = json.loads(response.text)
                self.new_change_id = response_json['ChangeId']
                return self.new_change_id
        else:
            return self.new_change_id
        

    def create_schema(self):
        # lets establish a connection by creating our connection class
        result = connector.establishConnection('HANA')
        if not result["success"]:
            return result
        self.hdb = result["data"]

        self.hr = HanaRest(self.base_url, self.username, self.password)

        # Instantiate our helper function
        self.hf = HelperFunctions(self.hdb)

        # Check if the schema already exists. If it does exists then simply exit with appropriate message
        schema_count = self.hf.get_schema_count(self.schema_name)
        if schema_count == 1:
            #Nothing to process, exit with message
            self.dict_schema_results = {
                "result": "Success",
                "result_code" : 0,
                "schema_name" : self.source_schema,
                "message" : "Schema already exists"
            }

        # Create the Schema
        else:
            result = self.create_new_schema(self.target_package_name)

            # Add the result to the outer result dict
            self.dict_schema_results = result

        print()
        print(json.dumps(self.dict_schema_results, indent=2))
        
        return {"success":True,"data":self.dict_schema_results}

    def create_new_schema(self, target_schema_package):

        tmp_dict_results = {}

        #Check/create the target calc view package 
        package_exists, schema_pkg_status = self.hr.check_if_package_exists(target_schema_package)
        
        #Check that the package was created successfully (or exists). 
        if package_exists == True:
            
            hdbschema_file_name = self.schema_name + ".hdbschema"
            
            hdbschema_file_content = f'''// Generated: {self.time_str}    \nschema_name = "{self.schema_name}";'''
            
            #Create the change list
            change_id = self.get_new_change_id()

            # Add the ChangeId to the list of Change IDs 
            self.dict_change_ids.setdefault(change_id)

            # Create the Schema file
            response = self.hr.write_file(target_schema_package, hdbschema_file_name, hdbschema_file_content, change_id)
                
            if response.status_code in (200, 202):
                result = "Success"
                result_code = 0
            else:
                result = "ERROR"
                result_code = 2

        else:
            result = "ERROR"
            result_code = 2

        tmp_dict_results = {
            "result": result,
            "result_code" : result_code,
            "schema_name" : self.schema_name
        }

        return tmp_dict_results


    def quit_with_message(self, change_id, message):
        self.dict_schema_results = {
            "result": "ERROR",
            "result_code" : 2,
            "message" : message,
            "change_id": change_id
        }

        print()
        print(json.dumps(self.dict_schema_results, indent=2))

        quit()

def createSchema(schema_name):
    # lets gets some env variables
    base_url=os.environ.get("hana_base_url")
    uid=os.environ.get("hana_uid")
    pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))
    
    # create our hana schema object
    hcs = HanaCreateSchema(base_url,uid,pwd,schema_name)
    result = hcs.create_schema()
    if not result["success"]:
        return result
    
    return {"success":True}     

if __name__ == "__main__":
    
    result = createSchema("DIETER_TEST_1_CL")
    if result["success"]:
        print("DONE")
    else:
        print(result["message"])
    