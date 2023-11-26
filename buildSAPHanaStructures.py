import pandas as pd
import numpy as np
import os
from os.path import exists
import hanaCreateSchema as cS
import hanaUpdateUnrestrictedCatalogRoles as uR
import hanaVitualiseTables as vsT
import hanaGenDPViews as dpV

def deployAllSAPHana(buildLst: list):
    # lets loop and execute each item
    for item in buildLst:
        target_schema = item["target_schema"]
        source_db = item["source_db"]
        target_package_name = item["target_package_name"]
        
        print(f"Processing:{target_schema} {source_db} {target_package_name}")
        
        # create the schema
        if len(str(target_schema).strip())>0:
            # call our schema creation
            result = cS.createSchema(str(target_schema).strip().upper())
            if not result["success"]:
                return {"success":False, "message":f"Schema Creation Failed - {result['message']}"}
        else:
            return {"success":False,"message":"Schema Creation Failed - Nothing to do since no target schema was provided"}

        # assign unrestricted roles
        result = uR.updateRoles()
        if not result["success"]:
            return {"success":False,"message":f"Unrestricted Roles Failed - {result['message']}"}
        
        # create virtual tables
        if len(str(target_schema).strip())>0 and len(str(source_db).strip())>0:
            # we call our create with just a like
            result = vsT.virtualiseTables(str(source_db).strip().lower(),str(target_schema).strip())
            # lets see if we had a success or not
            if not result["success"]:
                return {"success":False,"message":f"Virtual Tables Creation Failed - {result['message']}"}
        else:
            n = ''
            if len(str(source_db).strip())==0:
                n = 'source database'
            if len(str(target_schema).strip())==0:
                if n == '':
                    n = f'{n}/target schema'
                else:
                    n = f'target schema'
            return {"success":False,"message":f"Virtual Tables Creation Failed - Nothing to do since no {n} was provided"}                    
        
        # create dp layer
        if len(str(target_schema).strip())>0 and len(str(target_package_name).strip())>0:
            # we call our create with just a like
            result = dpV.generateDPViews(str(target_schema).strip().upper(),str(target_package_name).strip())
            if not result["success"]:
                return {"success":False,"message":f"DP Layer Creation Failed - {result['message']}"}
        else:
            n = ''
            if len(str(target_schema).strip())==0:
                n = 'source schema (SAP Hana)'
            if len(str(target_package_name).strip())==0:
                if n == '':
                    n = f'{n}/target package name'
                else:
                    n = f'target package name'
            return {"success":False,"message":f"DP Layer Creation Failed - Nothing to do since no {n} was provided"}
            
        print(f"Processed:{target_schema} {target_schema} {target_package_name}\n")
    
    return {"success":True}

def getSAPHanaInput(target_schema,source_db,target_package_name):
    # lets do some tests
    if len(str(target_schema).strip())<=0:
        return {"success":False,"message":"Target Schema must have a value"}
    if len(str(source_db).strip())<=0:
        return {"success":False,"message":"Source Database must have a value"}
    if len(str(target_package_name).strip())<=0:
        return {"success":False,"message":"Target Package Name must have a value"}
    
    # build ir list item from the input given
    buildLst = []
    item = {"target_schema":target_schema,"source_db":source_db,"target_package_name":target_package_name}
    buildLst.append(item)

    # now that we have our list lets execute it on SAP Hana
    result = deployAllSAPHana(buildLst)
    if not result["success"]:
        return result
    
    return {"success":True}    

def getSAPHanaInputFile(filename):
    # first lets see if the file exists or not
    if not exists(filename):
        return {"success":False,"message":f"File does not exits {filename}"}    
    
    # lets see if we have a csv file or an excel file
    split_fn = os.path.splitext(filename)
    
    # extract the file name and extension
    file_name = split_fn[0]
    file_extension = split_fn[1]
    
    # test our extension
    if len(str(file_extension).strip())<=0:
        return {"success":False,"message":"Cannot determine file type extension should be either .csv/.xls or .xlsx"}
    elif str(file_extension).strip().lower() == '.csv':
        df = pd.read_csv(filename)
    elif str(file_extension).strip().lower() == '.xls' or str(file_extension).strip().lower() == '.xlsx':
        df = pd.read_excel(filename)
    else:
        return {"success":False,"message":f"File type extension {file_extension} not supported it should be either .csv/.xls or .xlsx"}
    
    # validate the columns of the input file is correct
    collist = ('target_schema','source_db','target_package_name')
    for i,col in enumerate(df.columns):
        if col not in collist:
            return {"success":False,"message":f"{col} should be one of the following target_schema,source_db,target_package_name"}
    # validate the list of required columns are in the file
    for col1 in collist:
        found = False
        for col2 in df.columns:
            if col1==col2:
                found = True
                break
        if not found:
            return {"success":False,"message":f"{col1} should be one of the columns in the file {filename}"}
    
    # we have our data in a dataframe lets convert it to our list structure
    buildLst = []
    for row in df.index:
        # create the empty structure and loop trough the columns to build it
        item = {}
        for i,col in enumerate(df.columns):
            item[col] = df[col][row]
        buildLst.append(item)

    # now that we have our list lets execute it on SAP Hana
    result = deployAllSAPHana(buildLst)
    if not result["success"]:
        return result
    
    return {"success":True}

# our starting point
if __name__ == "__main__":
    getSAPHanaInputFile("c:/Dev/Excel_Data_Types/Book1.csv")