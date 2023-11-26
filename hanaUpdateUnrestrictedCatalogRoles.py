import json
import time
import hashlib
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
class UpdateUnrestrictedCatalogRoles():

    time_str = datetime.now().strftime("%Y%m%d %H:%M:%S")

    dict_role_results = {}

    def __init__(
            self,
            base_url,
            username,
            password,
            jira_number = 'TEST'):
        
        # set our values
        self.base_url = base_url
        self.username = username
        self.password = password
        self.jira_number = jira_number
        

    def update_roles(self):
        # lets establish a connection by creating our connection class
        result = connector.establishConnection('HANA')
        if not result["success"]:
            return result
        self.hdb = result["data"]

        self.hr = HanaRest(self.base_url, self.username, self.password)

        # Instantiate our helper function
        self.hf = HelperFunctions(self.hdb)

        self.target_package_id = "bi.sandbox.G986815.Dev.security.roles.functional.objects.dynamic"

        # Store the names of each role 
        role_names = ["SanlamHanaUnrestrictedCatalogRead", "SanlamHanaUnrestrictedCatalogProvision", "SanlamHanaUnrestrictedCatalogChange"]

        print()
        print("Processing Unrestricted Catalog roles ..")
        print()

        # Update the roles
        for role_type in [1,2,3]:
            role_name = role_names[role_type - 1]
            result = self.update_individual_role(role_name, role_type)
            self.dict_role_results[role_name] = result
                
        print(json.dumps(self.dict_role_results, indent=2))
        
        return {"success":True,"data":self.dict_role_results}

    def update_individual_role(self, role_name, role_type):

        # Get the Updated role definition
        role_cotent = self.hf.get_unrestricted_role_def(role_type)

        # Make sure something was returned
        if len(role_cotent) > 0:

            for row in role_cotent:
                new_role_content = row['PRIV']

                # Get the current role content so we can compare with the new role generated
                # to determine if we should update it (no need to blindly update all roles, some may not have changed)
                current_role_content = self.hr.read_file(self.target_package_id, role_name + ".hdbrole")
                
                # Remove the flowerboxes from the roles (first 5 lines) since they contain the date the role was generated
                # and this will always change. The Flower boxes are non functional anyway
                curr_compare = current_role_content.partition("--END HEADER--")
                new_compare = new_role_content.partition("--END HEADER--")

                # Compute the md5 hash of the current and new roles
                curr_hash = hashlib.md5(curr_compare[2].encode()).hexdigest()
                new_hash = hashlib.md5(new_compare[2].encode()).hexdigest()

                if curr_hash == new_hash:
                    result = "No changes detected"
                else:                
                    # Role has changed! Update the Role
                    response = self.hr.write_file(self.target_package_id, role_name + ".hdbrole", new_role_content, None)
                    if response.status_code == 200:
                        # Role successfully created/updated.
                        result = "Role changes detected!!! Processing ... Sucess"
                    else:
                        response_json = json.loads(response.text)
                        result = "Role changes detected!!! Processing ... ERROR -> " + response_json['errorMsg']

        return result


    def quit_with_message(self, change_id, message):
        self.dict_role_results = {
            "result": "ERROR",
            "result_code" : 2,
            "message" : message,
            "cnt_success" : self.cnt_success,
            "cnt_error" : self.cnt_error,
            "change_id": change_id
        }

        print()
        print(json.dumps(self.dict_role_results, indent=2))

        quit()

def updateRoles():
    # lets gets some env variables
    base_url=os.environ.get("hana_base_url")
    uid=os.environ.get("hana_uid")
    pwd=buildTools.decodetxt(os.environ.get("hana_pwd"))

    # create our hana dp view object
    uucr = UpdateUnrestrictedCatalogRoles(base_url,uid,pwd)
    result = uucr.update_roles()
    if not result["success"]:
        return result
    
    return {"success":True}

if __name__ == "__main__":
    result = updateRoles()
    if result["success"]:
        print("DONE")
    else:
        print(result["message"])
    