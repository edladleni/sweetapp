import os
import shutil
import json
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from jinja2 import Environment, BaseLoader
from base64 import b64decode
from base64 import b64encode
from datetime import datetime
import configparser

class config:
    def load_config(self, configpath):
        # create the object
        config = configparser.ConfigParser()
        config.read(f'{configpath}/deat.ini')
        
        # set the local variables
        # folders
        self.folder_template=config['folders']['folder_template']
        self.folder_mapping=config['folders']['folder_mapping']
        self.folder_input=config['folders']['folder_input']
        self.folder_output=config['folders']['folder_output'] 
        self.folder_archive=config['folders']['folder_archive']
        self.folder_test=config['folders']['folder_test']
        #files
        self.blacklist_dp=config['files']['blacklist_dp']
        self.whitelist_dp=config['files']['whitelist_dp']
        self.ignorelist_dp=config['files']['ignorelist_dp']
        self.blacklist_vir=config['files']['blacklist_vir']
        self.whitelist_vir=config['files']['whitelist_vir']
        self.ignorelist_vir=config['files']['ignorelist_vir']
        
        # options
        self.file_action=config['options']['file_action']
        # version
        self.toolset_version=config['version']['toolset_version']
        
    def __init__(self,configpath):
        self.load_config(configpath)

def escapeHana(text: str):
    return ' '.join(text.replace("'", "''").split('\n'))

def escapeJSON(text: str):
    return '\\n'.join(text.replace('\\', '\\\\').replace('"', '\\"').split('\n')).replace('\t', '\\t')

def encodetxt(text: str):
    try:
        result = b64encode(text.encode('utf-8')).decode('utf-8')
    except:
        result = text 
    return result
    
def decodetxt(text: str):
    try:
        result = b64decode(text).decode('utf-8')
    except:
        result = text
    return result

# archive a file
def archiveFile(fileinput,archivefolder,appendate=True,workload='',src=''):
    # no archive folder given we exit
    if len(archivefolder)<=0:
        return False
    # lets ensure that the archive folder exists else we create it
    os.makedirs(archivefolder, exist_ok=True)
    # so lets extract the file name outr of the 
    newfilename = os.path.basename(fileinput)
    if len(newfilename.strip())>0:
        f_name = os.path.splitext(newfilename)[0] 
        f_ext = os.path.splitext(newfilename)[1]
        
        #create the arhive name
        timeslot = ''
        if appendate:
            timeslot = "_" + datetime.now().strftime("%Y%m%d%H%M%S")
        if len(workload)>0:
            workload ='\\' + workload+'\\'+ src
        archivefilename = f"{archivefolder}{workload}\\{f_name}{timeslot}{f_ext}"
        shutil.move(fileinput,archivefilename)

# delete a file
def deleteFile(fileinput):
    os.remove(fileinput)

# this function checks to see if the a file exists or not and then load it into a string or alternative it can 
# cast it as json structure
def loadInputFiletoString(fileinput,makejson=False):
    # first lets see if the file exist or not
    if not os.path.isfile(fileinput):
        return {"success":False,"message":f"File does not exists {fileinput}"}

    # lets load the file into a string 
    with open(fileinput, 'r') as file:
        stringvalue = file.read()

    # we see if it is reuiqred to jsonify the string
    if makejson:
        data = json.loads(stringvalue)
    else:
        data = stringvalue

    return {"success":True,"message":f"Successfully loaded the file {fileinput}","data":data}

# this function checks to see if the a file exists or not and then load it into a list
def loadInputFiletoList(fileinput):
    # first lets see if the file exist or not
    if not os.path.isfile(fileinput):
        return {"success":False,"message":f"File does not exists {fileinput}"}

    # lets load the file into a list 
    with open(fileinput, 'r') as file:
        data = file.read()

    listdata = data.split("\n")

    return {"success":True,"message":f"Successfully loaded the file {fileinput}","data":listdata}


# this function creates the output bassed upon the template and the mapped information
def populateTemplateFromMap(processtemplate,processmapping,outputtype='JSON',outputfilename='',schemafilename=''):
    # lets us now 
    # now that we have all the values mapped lets execute our template engine
    template = Environment().from_string(processtemplate)
    data = template.render(values=processmapping)

    # we check to see if a file name was given
    if len(outputfilename.strip())>0:
        # lets aso write the result to an output file
        outputfile = open(outputfilename, "w")
        outputfile.write(data)
        outputfile.close()        
        
        # lets see what is the type we working if it is avro we can convert it from json structure
        if outputtype.upper()=='AVRO':
            # read the schema
            schema = avro.schema.parse(open(schemafilename).read())
            # create the avro file from the json file
            writer = DataFileWriter(open(f"{outputfilename}.avro","wb"), DatumWriter(), schema)
            writer.append(json.loads(data))
            writer.close()

    return {"success":True,"message":"Successfully rendered the output from the template","data":data}

# this function purpose is to get a list of tables and apply the different lists to determine which tables to use
# the process 
# 1. You must supply at minimum the schema/database.
# 2. You can supply a whitelist which will contain either a table or a wildcard.
# 3. You can supply a blacklist which will contain either a table or a wildcard.
# 4. If no backlist/whitelist is provided it means all tables residing in the schema
# 5. If whitelist/blacklist is provided the order will be see if table is in whitelist, then see if table is in backlist.
# 6. If a table is in both lists the blacklist will receive precedence.
# 7. Also, in the general will can have a list of table wildcards that will always be excluded irrespective if they are present in the white/blacklists.
def buildUseList(tablelist,whitelist=[],blacklist=[],ignorelist=[]):
    def containsValue(value, find):
        # lets upper our values
        value = value.upper()
        find = find.upper()
        # lets see what type of find we should do
        if find[0] == '%' and find[-1] == '%':
            # we see if we find a portion of the value
            findvalue = find[1:len(find)-1]
            if value.find(findvalue) != -1:
                return True
        elif find[0] != '%' and find[-1] == '%':
            # we compare the first part of the value
            findvalue = find[0:len(find)-1]
            comparevalue = value[0:len(findvalue)]
            if comparevalue==findvalue:
                return True
        elif find[0] == '%' and find[-1] != '%':
            # we compare the last part of the value
            findvalue = find[1:len(find)]
            comparevalue = value[len(value)-len(findvalue):]
            if comparevalue==findvalue:
                return True
        else:
            # no wildcards so we compare the value as is
            if value==find:
                return True
        
        return False
        
    uselist = []
    # lets loop trough out input list
    for tableitem in tablelist:
        # lets reset our flags
        wdefault = False
        wfind = False
        witemvalue = ''
        bfind = False
        ifind = False
        additem = False
        # lets loop trough the whitelist if we have one
        if len(whitelist)==0:
            wdefault = True
        else:
            for witem in whitelist:
                # lets see if 
                if containsValue(tableitem, witem):
                    print(f"Whitelist Item Found: {witem} in {tableitem}")
                    wfind = True
                    witemvalue = witem
                    break
        # next we loop trough our blacklist
        if len(blacklist)>0:
            for bitem in blacklist:
                # lets see if 
                if containsValue(tableitem, bitem):
                    print(f"Blacklist Item Found: {bitem} in {tableitem}")
                    bfind = True
                    break
        # next we see if we have an entry in our ignore list - this is like a blacklist but with general items to ignore
        if len(ignorelist)>0:
            for iitem in ignorelist:
                # lets see if 
                if containsValue(tableitem, iitem):
                    print(f"Ignore Item Found: {iitem} in {tableitem}")
                    ifind = True
                    break

        # after all our checks lets see if we can set the add item flag
        if not bfind and (wdefault or wfind):
            if ifind:
                if witemvalue.find('%')==-1 and not wdefault:
                    additem = True
            else:
                additem = True

        # lets add if we can
        if additem:
            print(f"Adding Item to List {tableitem}")
            uselist.append(tableitem)

    return {"success":True,"data":uselist}

# this function takes a string input value and determine the data type of the string
# current supported data types:
# str, int and float
def checkStringDataType(value):
    # lets see if what we working with
    # integer first
    try:
        int(value)
        return int
    except ValueError:
        pass
    # float value
    try:
        float(value)
        return float
    except ValueError:
        pass

    return str

