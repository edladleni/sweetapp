import os
import shutil
from time import sleep
from tqdm import tqdm
import maskpass
import pyperclip as pc
import buildTools
import common_dblib as connector
import buildUnstructuredfromExcel as bldE
import buildUnstructuredfromJSON as bldJ
import deployHive as depH
import buildReformattedExcelDoc as bldRE
import hanaCreateSchema as cS
import hanaUpdateUnrestrictedCatalogRoles as uR
import hanaVitualiseTables as vsT
import hanaGenDPViews as dpV
import buildSAPHanaStructures as sapH

# main menu options
menuitems ={
    1: "Process Flows - End to end processing",
    2: "Create JSON Document from Mapping Document",
    3: "Create Hive DDL from Mapping Document - Main/Raw/Test",
    4: "Create Avro Schema from Mapping Document",
    5: "Create Avro Schema from JSON Document",
    6: "Deploy Hive Script(s) - Main/Raw",
    7: "Deploy Hive Scripts in Folder - Main/Raw",
    8: "Create Schema in SAP HANA",
    9: "Assign Unrestricted Catalog Rights in SAP HANA",
    10: "Create Virtualization in SAP HANA",
    11: "Create DP Layer in SAP HANA",
    12: "Build all SAP HANA Structures using an input file (input file can contain multiple data structures to virtualized)",
    13: "Reformat an Excel Document (data types)",
    14: "Housekeeping - Clear Output Folder",
    15: "Utils - Encode Password",
    16: "Utils - Test Cloudera Access",
    17: "Utils - Test SAP Hana Access",
    18: "Exit"
}

# our process flow items
flowitems ={
    1: "Mapping Document to Cloudera",
    2: "Cloudera to SAP Hana",
    3: "Mapping Document to SAP Hana",
}

# create the header       
def create_header():
    baselength = 26
    templatename = f"{os.getcwd()}\\{cfg.folder_template}"
    mappingrulename = f"{os.getcwd()}\\{cfg.folder_mapping}"
    length = len(templatename) + baselength
    if len(mappingrulename) + baselength > length:
        length = len(mappingrulename) + baselength
    
    print("".join(['-' for i in range(1,length)]))
    print(f"- DEA Tools - Version {cfg.toolset_version}","".join([' ' for i in range(1,length - (baselength + len(cfg.toolset_version)))]), " -")
    print(''.join(['-' for i in range(1,length)]))
    print(f"Working Folder: {os.getcwd()}")
    print(f"Template Folder: {templatename}")
    print(f"Mapping Rules Folder: {mappingrulename}")
    print("".join(['-' for i in range(1,length)]))

# create the options from the menu
def create_menu():
    # create the menu structure
    for item in menuitems:
        print(f"{item}). --- {menuitems[item]}")

# create the options from the menu
def create_flows():
    # create the menu structure
    for item in flowitems:
        print(f"{item}). --- {flowitems[item]}")

# test connections
def test_connections(connectiontype):
    pwd = ''
    uid = input(f"Supply your logon user for {connectiontype} (empty value indicates the .env variables will be used):")
    if len(uid)>0:
        pwd = maskpass.askpass()
        if len(pwd)<=0:
            print("No password was given")
            return False
        
    result = connector.establishConnection(connectiontype,uid,pwd)
    if not result["success"]:
        print("Connecion Failed:")
        print(result["message"])
        return False
    else:
        print("Connecion Successfull")
        connector.closeConnection(result["data"])
        return True
    
# generic function to create the list of files to process
def create_filetoprocess(fileinfo,fileextension="*",default_path='',file_filter=''):
    # first get the file that we want to process
    filelist = []
    i = 0
    current_path = ""
    if len(default_path)>0:
        current_path = os.getcwd() + "\\" + default_path
    else:
        current_path = os.getcwd()
    for path in os.listdir(current_path):
        if os.path.isfile(os.path.join(current_path, path)):
            additem = False
            # lets get the files with a valid extension and files that fits the file filter criteria
            if path.find(fileextension) > 0 or fileextension == "*":
                if len(file_filter) > 0:
                    if path.find(file_filter) > 0:
                        additem = True
                else:
                    additem = True
                
            if additem:
                if not i:
                    print(f"Select {fileinfo} file to process from the list below or supply the name of the file (including the path)")
                i+=1
                print(f"{i}). --- [{path}]")
                filelist.append(current_path + "\\" + path)

    # now that we have files lets ask what file to process
    if not i:
        fileprocess = input(f"Name of {fileinfo} file to process:")
    else:
        fileprocess = input("Selection from list or name of file to process:")
        if ((fileprocess.strip()).isnumeric()):
            if len(filelist) < int(fileprocess.strip()):
                print(f"Invalid selection,valid selections 1 to {len(filelist)}")
                return {"success":False}
            fileprocess = filelist[int(fileprocess.strip()) - 1]
    
    # some basic checks
    if len(fileprocess) <= 0:
        return {"success":False}
    # check if the file exists or not
    fileexists = os.path.exists(fileprocess)
    if not fileexists:
        print(f"The {fileinfo} file selected does not exists - {fileprocess}")
        return {"success":False}

    # lets return        
    return {"success":True,"data":{"full_file":fileprocess,"file_name":fileprocess.replace(current_path,"")}}

# lets get the 
def get_files(title,input_type='Folder',file_extensions='*'):
    import tkinter as tk
    from tkinter import filedialog
    
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost',1)
    
    # file types setup
    if isinstance(title,tuple):
        filetypes = title        
    else:
        filetypes=((f'{title}',f'*.{file_extensions}'),)
    
    # we see which dialog box to display
    if input_type=='Folder':
        output_value = filedialog.askdirectory(title=f'Select Folder for {title}',initialdir=os.getcwd())
    elif input_type=='File':
        output_value = filedialog.askopenfilename(title=f'Select {title}',initialdir=os.getcwd(), filetypes=filetypes)
    elif input_type=='Files':
        output_value = filedialog.askopenfilenames(title=f'Select {title}',initialdir=os.getcwd(), filetypes=filetypes)
    else:
        return {"success":False,"message":f"Incorrect Input Type {input_type}"} 
    
    # lets see if we have a value or not
    if len(output_value)<=0:
        return {"success":False,"message":"No script selected"}    
    
    return {"success":True,"data":output_value}

# the function to use when we have a excel document that we want to create any of either json, ddl from
def createOutputfromExcel(mappingdocument,templatefile,mapfile):
    # some defaults
    templatestrings = []
    mappingstruct = ""
    mappingvalues = ""
    lastmessage = ""
    outputfolder = ""
    step = 1
    for i in tqdm(range(0,4), desc="Progress"):
        if step==1:
            # load the template(s)
            templates = []
            if isinstance(templatefile,str):
                templates.append({"filename":templatefile,"type":"other"})
            elif isinstance(templatefile,list):
                templates = templatefile
                
            # now that we have our templates lets loop trough them and get the details
            for single_template in templates:
                result = buildTools.loadInputFiletoString(single_template["filename"]) 
                if not result["success"]:
                    return result
                templatestrings.append({"value":result["data"],"type":single_template["type"]})
        if step==2:
            # load the mapping struct
            result = buildTools.loadInputFiletoString(mapfile,True) 
            if not result["success"]:
                return result
            mappingstruct = result["data"]
        if step==3:
            # lets create an output using the mapping document as input and the mappings
            # which will create an output from
            result = bldE.buildMapFromDoc(mappingdocument, mappingstruct)
            if not result["success"]:
                return result
            mappingvalues = result["data"]
        if step==4:
            # we might have multiple templates so we process each template string
            for templatestring in templatestrings:
                # lets now that we have mapped the data lets create the output using the template
                # we might have multiple objects that we need to create an output for each of them
                for i,item in enumerate(mappingvalues):
                    # create the name of the file and the file folder structure to be used
                    outputtype = str(mappingvalues[i]['values']['output_type']).strip()
                    outputpath = f"{os.getcwd()}/{cfg.folder_output}/{str(mappingvalues[i]['values']['workload_lower']).strip()}/{str(mappingvalues[i]['values']['source_lower']).strip()}/"
                    os.makedirs(outputpath, exist_ok=True)
                    if templatestring["value"]=="other":
                        outputfilename = f"{outputpath}{str(mappingvalues[i]['values']['workload_lower']).strip()}_{str(mappingvalues[i]['group_name']).strip()}.{str(mappingvalues[i]['values']['output_type']).strip()}"
                    else:
                        outputfilename = f"{outputpath}{templatestring['type']}_{str(mappingvalues[i]['values']['workload_lower']).strip()}_{str(mappingvalues[i]['group_name']).strip()}.{str(mappingvalues[i]['values']['output_type']).strip()}"
                    result = buildTools.populateTemplateFromMap(templatestring["value"],item["values"],outputtype,outputfilename)
                    if not result["success"]:
                        return result
                    # lets print it out to stdout
                    print('-------',''.join(['-' for lst in str(mappingvalues[i]['group_name'])]))
                    print(f"output={str(mappingvalues[i]['group_name'])}:")
                    print('-------',''.join(['-' for lst in str(mappingvalues[i]['group_name'])]))
                    print(f"{result['data']}\n\n")
                    
                    outputfolder = outputpath.replace("\\","/")
                    lastmessage = f"Output has been generated in folder [{outputfolder}]"
            
        step+=1        
        sleep(1)
    
    # i love it when a plans come together so lets print the last message that has been created if everything was successfull
    print(lastmessage)
    
    # lastly lets return a success
    return {"success":True,"data":outputfolder}

# the function to use when we have a excel document that we want to create any of either json, ddl from
def createOutputfromJSON(jsondoc,templatefile,mapfile):
    # some defaults
    templatestring = ""
    mappingstruct = ""
    mappingvalues = ""
    outputfolder = ""
    lastmessage = ""
    step = 1
    for i in tqdm(range(0,5), desc="Progress"):
        if step==1:
            # load the template
            result = buildTools.loadInputFiletoString(templatefile) 
            if not result["success"]:
                return result
            templatestring = result["data"]
        if step==2:
            # load the mapping struct
            result = buildTools.loadInputFiletoString(mapfile,True) 
            if not result["success"]:
                return result
            mappingstruct = result["data"]
        if step==3:
            # load the json data from the json document
            result = buildTools.loadInputFiletoString(jsondoc,True) 
            if not result["success"]:
                return result
            jsondata = result["data"]
        if step==4:
            # lets create an output using the mapping document as input and the mappings
            # which will create an output from
            result = bldJ.buildMapFromJSON(jsondata, mappingstruct)
            if not result["success"]:
                return result
            mappingvalues = result["data"]
        if step==5:
            # lets now that we have mapped the data lets create the output using the template
            # we do the loop of fate
            for i,item in enumerate(mappingvalues):
                # create the name of the file and the file folder structure to be used
                outputtype = 'avsc'
                outputpath = f"{os.getcwd()}/{cfg.folder_output}/{str(mappingvalues[i]['values']['workload_lower']).strip()}/{str(mappingvalues[i]['values']['source_lower']).strip()}/"
                os.makedirs(outputpath, exist_ok=True)
                outputfilename = f"{outputpath}{str(mappingvalues[i]['values']['workload_lower']).strip()}_{str(mappingvalues[i]['group_name']).strip()}.avsc"
                result = buildTools.populateTemplateFromMap(templatestring,item["values"],outputtype,outputfilename)
                if not result["success"]:
                    print(result["message"])
                    exit()
                # lets print it out to stdout
                print('-------',''.join(['-' for lst in str(mappingvalues[i]['group_name'])]))
                print(f"output={str(mappingvalues[i]['group_name'])}:")
                print('-------',''.join(['-' for lst in str(mappingvalues[i]['group_name'])]))
                print(f"{result['data']}\n\n")
                
                outputfolder = outputpath.replace("\\","/")
                lastmessage = f"Output has been generated in folder [{outputfolder}]"
            
        step+=1        
        sleep(1)
    
    # i love it when a plans come together so lets print the last message that has been created if everything was successfull
    print(lastmessage)
    
    # lastly lets return a success
    return {"success":True,"data":outputfolder}
    
def getMapDocInputs(task):
    # lets get the files to process
    # 1. the mapping document firstly
    result = create_filetoprocess("Mapping Document (Excel)","xlsx",cfg.folder_input)
    if not result["success"]:
        print("No Mapping Document File selected")
        return {"success":False}
    mappingdocument = result["data"]["full_file"]
    print("\n")
        
    # 2. the template to use
    # lets first create a filter
    file_filter = ""
    if task=='file ingestion':
        file_filter = "file_ingestion"
    elif task=='hive data structure':
        file_filter = "hive_data_structure"
    elif task=='avro schema':
        file_filter = "avro_schema"
    # lets get the template now
    result = create_filetoprocess("Template","template",cfg.folder_template,file_filter)
    if not result["success"]:
        print("No Template File selected")
        return {"success":False}
    # if we selected option 2 (hive data structures) then we need to create multiple templates to use
    if task=="hive data structure":
        templatefile = []
        if result["data"]["full_file"].find("_main."):
            templatefile.append({"filename":result["data"]["full_file"],"type":"main"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_main.","_raw."),"type":"raw"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_main.","_test_raw."),"type":"test"})
        elif result["data"]["full_file"].find("_test_raw."):
            templatefile.append({"filename":result["data"]["full_file"],"type":"test"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_test_raw.","_main."),"type":"main"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_test_raw.","_raw."),"type":"raw"})
        elif result["data"]["full_file"].find("_raw."):
            templatefile.append({"filename":result["data"]["full_file"],"type":"raw"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_raw.","_main."),"type":"main"})
            templatefile.append({"filename":result["data"]["full_file"].replace("_raw.","_test_raw."),"type":"test"})
    else:
        templatefile = result["data"]["full_file"]
    file_filter = result["data"]["file_name"].replace(".template","")
    file_filter = file_filter.replace("_main","")
    file_filter = file_filter.replace("_test_raw","")
    file_filter = file_filter.replace("_raw","")
    file_filter = file_filter.replace("t_","")
    file_filter = file_filter.replace("\\","")
    print("\n")

    # 3. mapping to to use
    result = create_filetoprocess("Map","json",cfg.folder_mapping,file_filter)
    if not result["success"]:
        print("No Map File selected")
        return {"success":False}
    mapfile = result["data"]["full_file"]
    print("\n")
    
    return {"success":True,"data":{"mappingdocument":mappingdocument,"templatefile":templatefile,"mapfile":mapfile}}
    
def genMappingDoc(task,gen=True,mappingdocument='',templatefile=[],mapfile=''):
    # get the input files
    if gen:
        result = getMapDocInputs(task)
        if not result["success"]:
            return result
        mappingdocument = result["data"]["mappingdocument"]
        templatefile = result["data"]["templatefile"]
        mapfile = result["data"]["mapfile"]
        
    result = createOutputfromExcel(mappingdocument,templatefile,mapfile)  
    if not result["success"]:
        return result
    return {"success":True,"data":result["data"]}
    
def genHiveDeployScript(gen=True,files=[]):
    # lets get the files to process
    # 1. the files first
    if gen:
        result = get_files(title='HQL Scripts',input_type='Files',file_extensions='hql')
        if not result["success"]:
            print(result["message"])
            return result
        files = result["data"]
            
    # 2. call our deploy function for scripts
    result = depH.deployScript(files,os.getcwd() + '/' + cfg.folder_archive,action=cfg.file_action)
    if not result["success"]:
        print(result["message"])
        return result
    print("Scripts executed succesfully")
    
    return {"success":True}

def genHiveDeployFolder(gen=True,folder='',processraw=True):
    # lets get the files to process
    # 1. the folder first
    if gen:
        result = get_files(title='HQL Scripts')
        if not result["success"]:
            print(result["message"])
            return result    
        folder = result["data"]
        raw = input("Do you want to process the RAW Scripts as well (Y/N) (Any other value equals to Y):")
        processraw = True
        if str(raw).strip().lower() == 'n':
            processraw = False
    
    # 2. call our deploy function for folders
    result = depH.deployFolder(folder,"hql",os.getcwd() + '/' + cfg.folder_archive,action=cfg.file_action,processraw=processraw)
    if not result["success"]:
        print(result["message"])
        return result
    print(f"Scripts executed succesfully in folder {folder}")
        
    return {"success":True}

def genSAPHanaSchema(gen=True,target_schema=''):
    if gen:
        target_schema = input("Please supply the target schema name (SAP Hana) :")
    if len(str(target_schema).strip())>0:
        # call our schema creation
        result = cS.createSchema(str(target_schema).strip().upper())
        if not result["success"]:
            print(result["message"])
            return result
    else:
        print(f"Nothing to do since no target schema was provided")
        return {"success":False,"message":"Nothing to do since no target schema was provided"}
    
    return {"success":True}

def genSAPHanaUnrestrictedRoles():
    result = uR.updateRoles()
        
    # lets see if we had a success or not
    if not result["success"]:
        print(result["message"])
        return result

    return {"success":True}

def genSAPHanaVirtualTables(gen=True,source_db='',target_schema='',table_like=''):
    if gen:
        source_db = input("Please supply the source database name (Cloudera) :")
        target_schema = input("Please supply the target schema name (SAP Hana) :")
        table_like = input("Please supply the table name like criteria (for all tables use %) :")
    if len(str(target_schema).strip())>0 and len(str(source_db).strip())>0 and len(str(table_like).strip())>0:
        # we call our create with just a like
        result = vsT.virtualiseTables(str(source_db).strip().lower(),str(target_schema).strip(),str(table_like).strip().lower() if len(str(table_like).strip())>0 else '%')
            
        # lets see if we had a success or not
        if not result["success"]:
            print(result["message"])
            return result
    else:
        n = ''
        if len(str(source_db).strip())==0:
            n = 'source database'
        if len(str(target_schema).strip())==0:
            if n == '':
                n = f'{n}/target schema'
            else:
                n = f'target schema'
        if len(str(table_like).strip())==0:
            if n == '':
                n = f'{n}/table name like'
            else:
                n = f'table name like'
        print(f"Nothing to do since no {n} was provided")
        return {"success":False,"message":f"Nothing to do since no {n} was provided"}

    return {"success":True}

def genSAPHanaDPLayer(gen=True,source_schema='',target_package_name='',table_like=''):
    if gen:
        source_schema = input("Please supply the source schema name (SAP Hana):")
        target_package_name = input("Please supply the target package name (SAP Hana):")
        table_like = input("Please supply the table name like criteria (for all tables use %)) - empty name implies using blacklist/whitelist:")
    if len(str(source_schema).strip())>0 and len(str(target_package_name).strip())>0:
        # depends on what has been selected we will call our create dp views
        if len(str(table_like).strip())>0:
            # we call our create with just a like
            result = dpV.generateDPViews(str(source_schema).strip().upper(),str(target_package_name).strip(),str(table_like).strip().upper() if len(str(table_like).strip())>0 else '%')
        else:
            # we call our create with the lists
            bl = f"{os.getcwd()}/{cfg.folder_input}/{cfg.blacklist_dp}"
            wl = f"{os.getcwd()}/{cfg.folder_input}/{cfg.whitelist_dp}"
            il = f"{os.getcwd()}/{cfg.folder_input}/{cfg.ignorelist_dp}"
            result = dpV.generateDPViewsFrom(str(source_schema).strip().upper(),str(target_package_name).strip(),bl,wl,il)
            
        # lets see if we had a success or not
        if not result["success"]:
            print(result["message"])
            return result
    else:
        n = ''
        if len(str(source_schema).strip())==0:
            n = 'source schema name'
        if len(str(target_package_name).strip())==0:
            if n == '':
                n = f'{n}/target package name'
            else:
                n = f'target package name'
        print(f"Nothing to do since no {n} was provided")
        return {"success":False,"message":f"Nothing to do since no {n} was provided"}

    return {"success":True}


def genSAPHanaStructFromFile():
    result = get_files(title=(('Excel Document','*.xlsx'),('Excel Document','*.xls'),('CSV Document','*.csv')),input_type='File')
    if not result["success"]:
        print(result["message"])
        return result
    inputf = result["data"]
    
    sapH.getSAPHanaInputFile(inputf)

def genReformattedExcel():
    # lets get the file to process
    # 1.a the excel document to be processed
    result = get_files(title='Excel Document to Reformat',input_type='File',file_extensions='xlsx')
    if not result["success"]:
        print(result["message"])
        return result
    inputf = result["data"]
    
    # 1.b process date columns
    datetime_cols = []
    datetime_perform=False
    ask_dates = input("Do you want to reformat date fields (Y/N) (Any other value equals to N):")
    if str(ask_dates).strip().lower() == 'y':
        datetime_perform=True
        datcolslst = input("Any additional columns to be reformatted (to date columns) (list each column seperated by a ,):")
        if len(str(datcolslst).strip())>0:
            datetime_cols = str(datcolslst).strip().split(',')
        print("\n")

    # 1.b process decimal columns
    decimal_cols = []
    decimal_perform=False
    ask_decimal = input("Do you want to reformat decimal fields (Y/N) (Any other value equals to N):")
    if str(ask_decimal).strip().lower() == 'y':
        decimal_perform=True
        deccolslst = input("Any additional columns to be reformatted (to decimal columns) (list each column seperated by a ,):")
        if len(str(deccolslst).strip())>0:
            decimal_cols = str(deccolslst).strip().split(',')
        print("\n")

    # 1.c process percentage columns
    percentage_cols = []
    percentage_perform=False
    ask_percentage = input("Do you want to reformat percentage fields (Y/N) (Any other value equals to N):")
    if str(ask_percentage).strip().lower() == 'y':
        percentage_perform=True
        percolslst = input("Any additional columns to be reformatted (to percentage columns) (list each column seperated by a ,):")
        if len(str(percolslst).strip())>0:
            percentage_cols = str(percolslst).strip().split(',')
        print("\n")
        
    # 2. do the reformatting
    result = bldRE.reformatExcel(inputf,datetime_perform=datetime_perform,datetime_cols=datetime_cols,decimal_perform=decimal_perform,decimal_cols=decimal_cols,percentage_perform=percentage_perform,percentage_cols=percentage_cols)
    if not result["success"]:
        print(result["message"])    
        return result
    
    print(f"The new output file can be viewed here:{result['data']}")
    
    openx = input("Do you want to view the newly created output file (Y/N) (Any other value equals to N):")
    if str(openx).strip().lower() == 'y':
        os.system(f"start excel {result['data']}")
    
    return {"success":True}    

def createFlowMappingtoCloudera():
    done = False
    while 1:
        # lets get the inputs first
        result = getMapDocInputs(task='hive data structure')
        if not result["success"]:
            return False
        mappingdocument = result["data"]["mappingdocument"]
        templatefile = result["data"]["templatefile"]
        mapfile = result["data"]["mapfile"]
        raw = input("Do you want to process the RAW Scripts as well (Y/N) (Any other value equals to Y):")
        processraw = True
        if str(raw).strip().lower() == 'n':
            processraw = False
            
        # lets see if we must continue or not
        print("\n")
        print("Are the following values correct?:")
        print(f"Mapping Document:{mappingdocument}")
        print(f"Template File:{templatefile[0]['filename']}")
        print(f"Mapping Rules File:{mapfile}")
        print(f"Process Raw Tables:{str(raw).strip().upper()}")
        val = input("Y/N/Any other value to Quit:")
        if type(val) is str:
            if val.strip().upper() == 'Y':
                break
            elif val.strip().upper() == 'N':
                continue
            else:
                done = True
                break
        else:
            done = True
            break
    
    # lets see if we must process or not
    if done:
        print("Mapping Document to Cloudera not Processed due to user request to quit")    
        return {"success":True} 
    
    # some process defaults
    output_folder = ""

    # start the process
    step = 1
    for i in tqdm(range(0,2), desc="Progress"):
        if step==1:
            result = genMappingDoc("hive data structure",False,mappingdocument,templatefile,mapfile)
            if not result["success"]:
                print("Generating Hive Data Structures Failed")
                return result
            output_folder = result["data"]
        if step==2:
            result = genHiveDeployFolder(False,folder=output_folder,processraw=processraw)
            if not result["success"]:
                print("Deploying of Hive Data Structures Failed")
                return result
            
        step+=1        
        sleep(1)
    
    # done with the processing
    print("Mapping Document to Cloudera Done")
    return {"success":True} 
   
def createFlowClouderatoSAPHana():
    done = False
    while 1:
        # first get all the inputs
        source_db = input("Please supply the source database name (Cloudera) :")
        target_schema = input("Please supply the target schema name (SAP Hana) :")
        table_like = input("Please supply the table name like criteria (for all tables use %)")
        target_package_name = input("Please supply the target package name (SAP Hana):")
    
        # lets see if we must continue or not
        print("\n")
        print("Are the following values correct?:")
        print(f"Source Database (Cloudera):{source_db}")
        print(f"Target Schema (SAP Hana):{target_schema}")
        print(f"Table Like criteria:{table_like}")
        print(f"Target Package Name (SAP Hana):{target_package_name}")
        val = input("Y/N/Any other value to Quit:")
        if type(val) is str:
            if val.strip().upper() == 'Y':
                break
            elif val.strip().upper() == 'N':
                continue
            else:
                done = True
                break
        else:
            done = True
            break
    
    # lets see if we must process or not
    if done:
        print("Cloudera to SAP Hana Process not Processed due to user request to quit")    
        return {"success":True} 
    
    # start the process
    step = 1
    for i in tqdm(range(0,4), desc="Progress"):
        if step==1:
            # create schema
            result = genSAPHanaSchema(False,target_schema)
            if not result["success"]:
                print("Schema Creation Failed")
                return result
        if step==2:
            # assign unrestricted roles
            result = genSAPHanaUnrestrictedRoles()
            if not result["success"]:
                print("Unrestricted Roles Failed")
                return result
        if step==3:
            # create virtual tables
            result = genSAPHanaVirtualTables(False,source_db,target_schema,table_like)
            if not result["success"]:
                print("Virtual Tables Creation Failed")
                return result 
        if step==4:
            # create dp layer
            result = genSAPHanaDPLayer(False,target_schema,target_package_name,table_like)
            if not result["success"]:            
                print("DP Layer Creation Failed")
                return result
            
        step+=1        
        sleep(1)
    
    # done with the processing
    print("Cloudera to SAP Hana Process Done")
    return {"success":True}

def createFlowMappingtoSAPHana():
    done = False
    while 1:
        # first get all the inputs
        result = getMapDocInputs(task='hive data structure')
        if not result["success"]:
            return False
        mappingdocument = result["data"]["mappingdocument"]
        templatefile = result["data"]["templatefile"]
        mapfile = result["data"]["mapfile"]
        raw = input("Do you want to process the RAW Scripts as well (Y/N) (Any other value equals to Y):")
        processraw = True
        if str(raw).strip().lower() == 'n':
            processraw = False
        source_db = input("Please supply the source database name (Cloudera) :")
        target_schema = input("Please supply the target schema name (SAP Hana) :")
        table_like = input("Please supply the table name like criteria (for all tables use %)")
        target_package_name = input("Please supply the target package name (SAP Hana):")
    
        # lets see if we must continue or not
        print("\n")
        print("Are the following values correct?:")
        print(f"Mapping Document:{mappingdocument}")
        print(f"Template File:{templatefile[0]['filename']}")
        print(f"Mapping Rules File:{mapfile}")
        print(f"Process Raw Tables:{str(raw).strip().upper()}")
        print(f"Source Database (Cloudera):{source_db}")
        print(f"Target Schema (SAP Hana):{target_schema}")
        print(f"Table Like criteria:{table_like}")
        print(f"Target Package Name (SAP Hana):{target_package_name}")
        val = input("Y/N/Any other value to Quit:")
        if type(val) is str:
            if val.strip().upper() == 'Y':
                break
            elif val.strip().upper() == 'N':
                continue
            else:
                done = True
                break
        else:
            done = True
            break
    
    # lets see if we must process or not
    if done:
        print("Mapping Document to SAP Hana Process not Processed due to user request to quit")    
        return {"success":True} 

    
    # some process defaults
    output_folder = ""

    # start the process
    step = 1
    for i in tqdm(range(0,6), desc="Progress"):
        if step==1:
            result = genMappingDoc("hive data structure",False,mappingdocument,templatefile,mapfile)
            if not result["success"]:
                print("Generating Hive Data Structures Failed")
                return result
            output_folder = result["data"]
        if step==2:
            result = genHiveDeployFolder(False,folder=output_folder,processraw=processraw)
            if not result["success"]:
                print("Deploying of Hive Data Structures Failed")
                return result
        if step==3:
            # create schema
            result = genSAPHanaSchema(False,target_schema)
            if not result["success"]:
                print("Schema Creation Failed")
                return result
        if step==4:
            # assign unrestricted roles
            result = genSAPHanaUnrestrictedRoles()
            if not result["success"]:
                print("Unrestricted Roles Failed")
                return result
        if step==5:
            # create virtual tables
            result = genSAPHanaVirtualTables(False,source_db,target_schema,table_like)
            if not result["success"]:
                print("Virtual Tables Creation Failed")
                return result 
        if step==6:
            # create dp layer
            result = genSAPHanaDPLayer(False,target_schema,target_package_name,table_like)
            if not result["success"]:            
                print("DP Layer Creation Failed")
                return result
            
        step+=1        
        sleep(1)
    
    # done with the processing
    print("Mapping Document to SAP Hana Done")
    return {"success":True} 

# our starting point
if __name__ == "__main__":
    # create our config object
    cfg = buildTools.config('./')
    
    # create our main menu
    while True:
        # clear the screen
        os.system('cls')
        # create the menu
        create_header()
        create_menu()
        
        # get selection
        option = ''
        try:
            option = int(input('Enter your choice:'))
        except:
            print(f'Invalid choice: Please enter a number between 1 and {len(menuitems)}')
        # clear line
        print("\n")
            
        # action selection
        if len(menuitems)==option:
            break
        elif option == 1:
            # print flow menu
            create_flows()
            
            # get selection
            fail = False
            flowoption = ''
            try:
                flowoption = int(input('Enter your choice:'))
            except:
                fail = True
                
            if fail or flowoption>3:
                print(f'Invalid choice: {flowoption}')
            else:
                if flowoption == 1:
                    createFlowMappingtoCloudera()
                elif flowoption == 2:
                    createFlowClouderatoSAPHana()
                elif flowoption == 3:
                    createFlowMappingtoSAPHana()
                else:
                    print("Option not yet available")

            # clear line
            print("\n")
            
        elif option in (2,3,4):
            # lets see what option was selected
            if option == 2:
                task = "file ingestion"
            elif option == 3:
                task = "hive data structure"
            else:
                task = "avro schema"
            genMappingDoc(task)
        elif option == 5:
            # lets get the files to process
            # 1. the json document as input first
            process = True
            result = create_filetoprocess("JSON Document","json",cfg.folder_input)
            if not result["success"]:
                print("No JSON Document File selected")
                process = False
            else:
                jsondoc = result["data"]["full_file"]
                print("\n")
                
            # 2. the template to use
            if process:
                # lets first create a filter
                file_filter = "avro_schema"

                # lets get the template now
                result = create_filetoprocess("Template","template",cfg.folder_template,file_filter)
                if not result["success"]:
                    print("No Template File selected")
                    process = False
                else:
                    templatefile = result["data"]["full_file"]
                    file_filter = result["data"]["file_name"].replace(".template","")
                    file_filter = file_filter.replace("t_","")
                    file_filter = file_filter.replace("\\","")
                    print("\n")

            # 3. mapping to to use
            if process:
                result = create_filetoprocess("Map","json",cfg.folder_mapping,file_filter)
                if not result["success"]:
                    print("No Map File selected")
                    process = False
                else:
                    mapfile = result["data"]["full_file"]
                    print("\n")
                
            if process:
                createOutputfromJSON(jsondoc,templatefile,mapfile)
        elif option == 6:
            genHiveDeployScript()
        elif option == 7:
            genHiveDeployFolder()
        elif option == 8:
            genSAPHanaSchema()
        elif option == 9:
            genSAPHanaUnrestrictedRoles()
        elif option == 10:
            genSAPHanaVirtualTables()
        elif option == 11:
            genSAPHanaDPLayer()
        elif option == 12:
            # sap hana excel file
            genSAPHanaStructFromFile()
        elif option == 13:
            genReformattedExcel()
        elif option == 14:
            # lets make sure that the user knows what he wants to do
            clear = input("Are you sure you want to clear the Output Folder - all data will be lost (Y/N):")
            if str(clear).upper() == 'Y':
                print("Clearing Output Folder....")
                # before we clear the path lets see if the output folder exists or not
                folderoutput = f"{os.getcwd()}/{cfg.folder_output}/"
                if os.path.exists(folderoutput):
                    shutil.rmtree(folderoutput)
                print("Output Folder Cleared.....")
        elif option == 15:
            pwd = maskpass.askpass()
            if len(pwd)>0:
                encodepwd=buildTools.encodetxt(pwd)
                pc.copy(encodepwd)
                print("Your Encoded Password to use: " + encodepwd)
                print("Encoded Password copied to clipboard......")
            else:
                print("No password given.......")
        elif option == 16:
            test_connections("HIVE")
        elif option == 17:
            test_connections("HANA")
        else:
            print(f'Invalid choice: Please enter a number between 1 and {len(menuitems)}')
            
        # user input to continue
        print("\n")
        input("Press any key to continue")                
          
    # clear the screen
    os.system('cls')

    print("Cheers " + os.getlogin())
