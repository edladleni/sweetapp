import json
import os
import re
from jinja2 import Environment, BaseLoader
import buildTools as bt

def buildMapFromJSON(jsondata,mappingdoc):
    # function that perform a certain action on the data
    def getActionValue(actionitem,value):
        returnValue = ""
        if actionitem=="upcase":
            returnValue = str(value).upper()
        elif actionitem=="lowcase":
            returnValue = str(value).lower()
        elif actionitem=="extract_alpha":
            returnValue = "".join(re.findall("[a-zA-Z]",str(value)))
        elif actionitem=="extract_numeric":
            returnValue = "".join(re.findall("[0-9]",str(value)))
        elif actionitem=="extract_precision":
            returnValue = "".join(re.findall("[0-9,.]",str(value)))
        
        return returnValue
    
    # function that gets the correct conditional value to apply
    def getConditionalValue(conditionitem,value):
        # always set the return value 
        returnValue = value
        for item in conditionitem:
            # lets use the original value to compare against
            comparevalue = value                
            # lets start the comparison
            if "equal" in item:
                if str(item["equal"]).strip()==str(comparevalue).strip():
                    returnValue = item["result"]
                    break
            if "in" in item:
                if str(comparevalue).strip() in str(item["in"]).strip():
                    returnValue = item["result"]
                    break
            if "last" in item and item["last"]:
                returnValue = item["result"]
        return returnValue

    # lets first process the globals
    print("")
    globals = {}
    if "globals" in mappingdoc:
        for item in mappingdoc["globals"]:
            if "input" in item:
                value = input(item["description"] + ":")
            if "start" in item:
                if "globals" in item["start"]:
                    value = globals[item["start"].split("::")[1]]
                else:
                    value = item["start"]
            
            # now that we have a vlaue lets see what to do with it
            if "action" in item:
                value = getActionValue(item["action"],value)

            # lastly lets add the global
            globals[item["label"]] = value

    # now we can actually do the conversion
    valuelist = []    
    for object in jsondata[mappingdoc["objects"]["identifier"]]:
        # for each object lets create the item as a group
        if mappingdoc["objects"]["group_by"]["start"] in object:
            # assign our group by and create the group
            objectname = object[mappingdoc["objects"]["group_by"]["start"]]
            valuegroup = {"group_name":objectname,"values":{"table":[]}}

            # now lets get all the mapping rules for the object and assign the values
            for mappingrule in mappingdoc["objects"]["mapping_rules"]:
                value = object[mappingrule["start"]]
                # now that we have a vlaue lets see what to do with it
                if "action" in mappingrule:
                    value = getActionValue(mappingrule["action"],value)

                # lets assign our value
                valuegroup["values"][mappingrule["label"]] = value

            # assign our globals to each group
            valuegroup["values"].update(globals)

            # create our rows of attributes
            rowlist = []
            for attributes in object[mappingdoc["attributes"]["identifier"]]:
                # create an empty row value object
                rowvalue = {}
                # now lets get all the mapping rules for the attribute and assign the values
                for mappingrule in mappingdoc["attributes"]["mapping_rules"]:
                    value = attributes[mappingrule["start"]]
                    # now that we have a vlaue lets see what to do with it
                    if "action" in mappingrule:
                        value = getActionValue(mappingrule["action"],value)
                    if "condition" in mappingrule:
                        value = getConditionalValue(mappingrule["condition"],value)
                    rowvalue[mappingrule["label"]] = value
                # lastly lets add the row values to our list of rows
                rowlist.append(rowvalue)

            # lastly we will see if we need to create linefeed items for each column that we mapped                
            itemcount = len(rowlist)
            for a,val in enumerate(rowlist):
                if a!=itemcount-1:
                    rowlist[a]["linefeed"]=','
                else:
                    rowlist[a]["linefeed"]=""

            valuegroup["values"]["table"]=rowlist
            valuelist.append(valuegroup)

    return {"success":True,"message":"Successfully created the output structure","data":valuelist}

if __name__ == "__main__":
    inputfile = f"{os.getcwd()}\\inputs\\TSTmodel.json"
    templatefile = f"{os.getcwd()}\\templates\\t_avro_schema.template"
    filemapping = f"{os.getcwd()}\\mappings\\m_json_to_avro_schema.json"

    # load the template
    result = bt.loadInputFiletoString(templatefile) 
    if not result["success"]:
        print(result["message"])
        exit()
    templatestring = result["data"]

    # load the mapping rules struct
    result = bt.loadInputFiletoString(filemapping,True) 
    if not result["success"]:
        print(result["message"])
        exit()
    mappingdoc = result["data"]


    # load the input struct
    result = bt.loadInputFiletoString(inputfile,True) 
    if not result["success"]:
        print(result["message"])
        exit()
    jsondata = result["data"]

    # lets create an outout using the mapping document as input and the mappings
    # which will create an output from
    result = buildMapFromJSON(jsondata, mappingdoc)
    if not result["success"]:
        print(result["message"])
        exit()

    print(f"{result['message']}:")
    valuelist = result['data']

    for i,item in enumerate(valuelist):
        # create the name of the file and the file folder structure to be used
        outputtype = 'avsc'
        outputpath = f"{os.getcwd()}\\outputs\\{str(valuelist[i]['values']['workload_lower']).strip()}\\{str(valuelist[i]['values']['source_lower']).strip()}\\"
        os.makedirs(outputpath, exist_ok=True)
        outputfilename = f"{outputpath}{str(valuelist[i]['values']['workload_lower']).strip()}_{str(valuelist[i]['group_name']).strip()}.avsc"
        result = bt.populateTemplateFromMap(templatestring,item["values"],outputtype,outputfilename)
        if not result["success"]:
            print(result["message"])
            exit()
        # lets print it out to stdout
        print('-------',''.join(['-' for lst in str(valuelist[i]['group_name'])]))
        print(f"output={str(valuelist[i]['group_name'])}:")
        print('-------',''.join(['-' for lst in str(valuelist[i]['group_name'])]))
        print(f"{result['data']}\n\n")
