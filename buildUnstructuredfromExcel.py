import pandas as pd
from pandas import ExcelWriter, ExcelFile
import os
import re
import json
import buildTools as bt

# this function do the following
# it creates a mapping output in json format from a mapping document
def buildMapFromDoc(filetoprocess, mapping):
    # we have a translation function that takes the excel cell
    # and convert it into x,y values
    def celltoPos(cell):
        # we determine which method to use since we can receive the value as either
        # a. x,y values
        # b. a excel cell like a1
        x = 0
        y = 0
        if "," in cell:
            lst = cell.split(",")
            if not len(lst)==2:
                return {"success":False,"message":f"No a valid x,y value {cell} - more than two values"}
            # lets see if the values are numeric
            if not lst[0].isnumeric():
                return {"success":False,"message":f"No a valid x,y value {cell} - x value {lst[0]}"}    
            if not lst[1].isnumeric():
                return {"success":False,"message":f"No a valid x,y value {cell} - x value {lst[1]}"}   
            # assign the values 
            x = int(lst[0])-1
            y = int(lst[1])-1
        else:
            lst = re.split('(?<=\D)(?=\d)', cell)
            if not len(lst)==2:
                return {"success":False,"message":f"No a valid cell {cell} - multiple values"}
            x = lst[1] # the x coordinate is the second element in the cell
            # check to see if the value is numeric only
            if not x.isnumeric():
                return {"success":False,"message":f"No a valid cell {cell} - x value {x}"}
            x = int(x)-1
            # check to see if the value is string only
            y = lst[0] # the y ccordinate is the first element in the cell
            # check to see if the value is alphabetic
            if not y.isalpha():
                return {"success":False,"message":f"No a valid cell {cell} - y value {y}"}
            # next we convert the into a numeric
            number = -25
            for i in y:
                number += ord(i.upper())-64+25
            y = number-1

        # return our values
        return {"success":True,"message":"Cell converted","data":{"x":int(x),"y":int(y)}}

    # function that gets the correct conditional value to apply
    def getConditionalValue(conditionitem,value,currentdf,currentrow):
        # always set the return value 
        returnValue = value
        for item in conditionitem:
            # we might have a test scenario that we do not want to use the value to test against 
            # that was passed so we retieve the alternative value to test against
            comparevalue = ""
            if "alter_index" in item:
                # get the alternative value
                comparevalue = currentdf.iloc[currentrow,item["alter_index"]-1]
            else:
                # lets use the original value to compare against since we do not have an alternative value to use
                comparevalue = value                
            
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
    def getFindandReplaceValue(farlist,value,currentdf,currentrow):
        # lets do the find and replace
        for faritem in farlist:
            # lets get the values required
            # we see what type of replacement do we have to do
            if "replace_index" in faritem:
                replacevalue = df.iloc[startx,faritem["replace_index"]-1]
            elif "replace_start" in faritem:
                result = celltoPos(faritem["replace_start"])
                if not result["success"]:
                    return value
                x = result["data"]["x"]
                y = result["data"]["y"]
                
                replacevalue = str(df.iloc[x,y])
            elif "replace_value" in faritem:
                replacevalue = faritem["replace_value"]
            # we have our replacement valure lets replace it
            if "find" in faritem:
                value = value.replace(faritem["find"],replacevalue)

        returnValue = value
       
        return returnValue
    
    # function that performs a user-defined regular expression against the data
    def getRegexValue(regexitem,value):
        returnValue = value
        # first lets see if we have a regular expression defined
        if "expression" in regexitem:
            returnValue = "".join(re.findall(regexitem["expression"],value))    
        return returnValue

    # function that validate the vlaue against the exclusion list
    def canExclude(excludeitem,value):
        returnValue = False
        # first lets see if we have a regular expression defined
        if "list" in excludeitem:
            # lets first see if we care for case
            validatecase = True
            if "validatecase" in excludeitem:
                if str(excludeitem["validatecase"]).lower()=="n":
                    validatecase = False
            # lets now validate our value against the items in the list
            for item in excludeitem["list"]:
                # the check if we must ignore case
                if validatecase and item==value:
                    returnValue = True
                    break
                # the check if we must ignore case
                if not validatecase and str(item).lower()==str(value).lower():
                    returnValue = True
                    break
        return returnValue

    # lets us load our mapping sheet in
    # lets read in the sheets first
    try:
        df_sheet_list = pd.read_excel(filetoprocess, sheet_name=None,header=None)
    except Exception as ex:
        return {"success":False,"message":f"Could not open {filetoprocess} {ex.args}"}
    
    # lets first see how may value list we have to create
    valuelist = []
    if "group_by" in mapping:
        # lets see which sheet to load
        for sheet in df_sheet_list:
            # lets make sure we have the sheet
            if str(sheet).strip().upper()==str(mapping["group_by"]["sheet"]).strip().upper():
                # lets create a dataframe for our sheet
                df = pd.DataFrame(df_sheet_list[sheet])
                df = df.fillna('')

                # get the starting point to determine the group
                result = celltoPos(mapping["group_by"]["start"])
                if not result["success"]:
                    return result
                x = result["data"]["x"]
                y = result["data"]["y"]

                maxx = len(df)-1

                # lets create the output
                row = True
                while row:
                    value = df.iloc[x,y]
                    if len(str(value).strip())<=0:
                        row = False
                    else:
                        valuegroup = {"group_name":value,"values":None}
                        found = False
                        for item in valuelist:
                            if item["group_name"]==value:
                                found = True
                                break
                        if not found:
                            valuelist.append(valuegroup)
                        x+=1

                        # lets see if the maxx exceeds the current
                        if maxx<x:
                            row = False
    else:
        # lets add a group
        valuegroup = {"group_name":"singelton","values":None}
        valuelist.append(valuegroup)

    # for each group we will create its own values
    for i,valuegroup in enumerate(valuelist):
        # lets now loop trough our mapping and then get the value from our excel mapping document
        values = {}
        for item in mapping["mapping"]:
            # lets get the sheet where the value should be
            for sheet in df_sheet_list:
                # lets make sure we have the sheet
                if str(sheet).strip().upper()==str(item["sheet"]).strip().upper():
                    # lets create a dataframe for our sheet
                    df = pd.DataFrame(df_sheet_list[sheet])
                    df = df.fillna('')

                    # lets get the x,y coordinates either it has been entered
                    # or it has been given as a excel cell thus we translate it
                    # this is only applicable for certain types not the group_by type
                    # which does not use a position
                    if not "group_by" in item["type"]:
                        result = celltoPos(item["start"])
                        if not result["success"]:
                            return result
                        x = result["data"]["x"]
                        y = result["data"]["y"]

                    # lets see what we need to do 
                    value = ""
                    exclude = False
                    if item["type"]=="value":
                        value = df.iloc[x,y]
                        # always check first if the value is in the exclusion list or not
                        if "exclusion" in item:
                            exclude = canExclude(item["exclusion"],value)
                        # lets see what we need to do
                        if not exclude:
                            if "action" in item:
                                value = getActionValue(item["action"],value)
                            if "regex" in item:
                                value = getRegexValue(item["regex"],value)
                            if "condition" in item:
                                value = getConditionalValue(item["condition"],value,df,x)
                            if "find_and_replace" in item:
                                value = getFindandReplaceValue(item["find_and_replace"],value,df,x)
                            
                    elif item["type"]=="group_by":
                        value = valuegroup["group_name"]
                        
                        # lets see what we need to do 
                        if "action" in item:
                            value = getActionValue(item["action"],value)
                        if "regex" in item:
                            value = getRegexValue(item["regex"],value)
                        if "condition" in item:
                            value = getConditionalValue(item["condition"],value,df,x)
                        if "find_and_replace" in item:
                            value = getFindandReplaceValue(item["find_and_replace"],value,df,x)
                        
                    elif item["type"]=="list":
                        # we start at one line at a time and first read the columns for the line
                        # we then move to the next line, if and end is specified then we use that rather
                        # then supply looping trough until you get an empty line

                        # lets set some values before we start
                        startx = x
                        starty = y
                        row = True
                        rowlist = []
                        maxx = len(df)-1
                        maxy = len(df.columns)-1
                        # if we have an end key we set our max values
                        if "end" in item:
                            result = celltoPos(item["end"])
                            if not result["success"]:
                                return result
                            maxx = result["data"]["x"]
                            maxy = result["data"]["y"]
                            # we also see that even if we have set and end point
                            # that our value does not exceed the limit of the sheet
                            if maxx>len(df)-1:
                                maxx = len(df)-1
                            if maxy>len(df.columns)-1:
                                maxy = len(df.columns)-1
                        # if we have headings then we set a indicator to indicate that even for an empty cell
                        # we still have to see if populate the value or not
                        haveheading = False
                        headingcount = 0
                        if "headinglabels" in item:
                            haveheading = True
                            headingcount = len(item["headinglabels"])
                        while row:
                            # set some values before we start
                            col = True
                            rowvalue = {}
                            colcount = 0
                            while col:
                                # get the value from the cell
                                if len(df.columns)-1>=starty:
                                    colcount+=1

                                    # get the value on the sheet
                                    value = df.iloc[startx,starty]
                                    # test to see if we can add it to the dict
                                    # first we see if have geadings set or not it then determines
                                    # how we going to see if need to exist or not
                                    if not haveheading and len(str(value).strip())<=0:
                                        col = False
                                    # 
                                    elif haveheading and headingcount<=starty:
                                        col = False
                                    else:
                                        # set our heading for this value or rather the dict key to use for this value
                                        heading = f"heading{colcount}"
                                        if "headinglabels" in item:
                                            heading = item["headinglabels"][colcount -1]

                                        # assign value
                                        rowvalue[heading] = value
                                        # move column to the next col
                                        starty+=1

                                else:
                                    col = False
                            # we have a complete row lets see if we add it or not, if its completely empty we exit
                            if len(rowvalue)>0:
                                # add value
                                rowlist.append(rowvalue)
                                # move marker to new line and reset column to the start position
                                startx = startx+1
                                starty = y
                            else:
                                row = False
                            
                            # lets see if we have reach the limit of rows the df contains
                            # or that we reach limit that has been set for us
                            if maxx<startx:
                                row = False

                        # last act to assign the list of items to our value
                        value = rowlist
                    elif item["type"]=="table":
                        # set some row values before we start
                        maxx = len(df)-1
                        startx = x
                        starty = y
                        row = True
                        rowlist = []
                        # we need to know if we a going to add this row values or not we
                        # only do that if we have a group by clause
                        group_by = False
                        if "group_by" in item:
                            group_by = True
                            groupbyindex = int(item["group_by"]["index"])
                            groupbyvalue = valuegroup["group_name"]

                        while row:
                            # well if the group by clause is satified then we do not need to work with this row at all
                            skiprow = False
                            if group_by:
                                value = df.iloc[startx,y+groupbyindex-1]
                                if value != groupbyvalue:
                                    # we set our flag to skip this row
                                    skiprow = True
                            if not skiprow:
                                # set some col values before we start
                                col = True
                                rowvalue = {}
                                colcount = 1
                                while col:
                                    # set our skip col flag
                                    skipcols = False
                                    # lets see if we reach the last column for us to go to the next row
                                    if colcount >= int(item["colcount"]):
                                        col = False
                                    # lets see if the current column we need to map or not
                                    # we find the mappings for the specific column if we do 
                                    # not have any we just skip this column
                                    for colmappings in item["headinglabels"]:
                                        if colmappings["index"]==colcount:
                                            # get value from sheet
                                            value = df.iloc[startx,starty]
                                            
                                            # lets see if the values is empty and if we set an alternative value to get
                                            if len(str(value).strip())<=0 and "alter_index" in colmappings:
                                                value = df.iloc[startx,colmappings["alter_index"]-1]
                                            
                                            # first things first lets see if we can skip this row or not by seeing if our value
                                            # is equals to any of our values in the exclusion list
                                            if "exclusion" in colmappings:
                                                skipcols = canExclude(colmappings["exclusion"],value)
                                            
                                            # lets see if we need to skip or not
                                            if not skipcols:
                                                # we can now do the rest of the processing
                                                if "action" in colmappings:
                                                    value = getActionValue(colmappings["action"],value)
                                                if "regex" in colmappings:
                                                    value = getRegexValue(colmappings["regex"],value)
                                                if "condition" in colmappings:
                                                    value = getConditionalValue(colmappings["condition"],value,df,startx)
                                                if "find_and_replace" in colmappings:
                                                    value = getFindandReplaceValue(colmappings["find_and_replace"],value,df,startx)

                                                # last part is to assign the value
                                                rowvalue[colmappings["label"]] = value

                                    # move our colcount to the next position
                                    colcount += 1
                                    starty += 1

                                # lets determine if we can exit
                                # we have a complete row lets see if we add it or not, if its completely empty we exit
                                if len(rowvalue)>0:
                                    # add value only if the skiprow has been set to false
                                    if not skiprow:
                                        rowlist.append(rowvalue)
                                    # move marker to new line and reset column to the start position
                                    startx += 1
                                    starty = y
                                else:
                                    row = False
                            else:
                                startx +=1
                            
                            # lets see if we have reach the limit of rows the df contains
                            # or that we reach limit that has been set for us
                            if maxx<startx:
                                row = False

                        # last act to assign the list of items to our value
                        value = rowlist

                    # if the value has a value we will map it
                    if not exclude and len(str(value).strip())>0:
                        # if we are working with a list or table lets see if we need to add a linefeed character
                        if item["type"] in ("list","table") and "linefeed" in item:
                            # lets loop through our items within the structure
                            # we get the length of our items first
                            itemcount = len(value)
                            for a,val in enumerate(value):
                                if a!=itemcount-1:
                                    value[a]["linefeed"]=item["linefeed"]
                                else:
                                    value[a]["linefeed"]=""

                        values[item["label"]]=value
                    else:
                        # lets see if the item is required and we do not have a value for it
                        if "required" in item and item["required"]:
                            return {"success":False,"message":f"The item is marked as required and no value present in mapping sheet:{item}"}
                        values[item["label"]]=None

        # lets add our general values as well for each group
        if "global" in mapping:
            for item in mapping["global"]:
                values.update(item)

        # now that we mapped it lets update our mapping values in the group
        valuelist[i]["values"]=values

    return {"success":True,"message":"Successfully created the output structure","data":valuelist}

if __name__ == "__main__":
    filetoprocess = f"{os.getcwd()}\\inputs\\supplementary.xlsx"
    filetemplate = f"{os.getcwd()}\\templates\\t_file_ingestion.template"
    filemapping = f"{os.getcwd()}\\mappings\\m_file_ingestion.json"
    fileext = "json"

    # load the template
    result = bt.loadInputFiletoString(filetemplate) 
    if not result["success"]:
        print(result["message"])
        exit()
    templatestring = result["data"]

    # load the mapping rules struct
    result = bt.loadInputFiletoString(filemapping,True) 
    if not result["success"]:
        print(result["message"])
        exit()
    mappingstruct = result["data"]

    # lets create an outout using the mapping document as input and the mappings
    # which will create an output from
    result = buildMapFromDoc(filetoprocess, mappingstruct)
    if not result["success"]:
        print(result["message"])
    else:
        print(f"{result['message']}:")

        # lets now that we have mapped the data lets create the output using the template
        # we do the loop of fate
        for i,item in enumerate(result["data"]):
            outputfilename = f"outputs/{str(result['data'][i]['values']['workload_lower']).strip()}_{str(result['data'][i]['group_name']).strip()}.{str(result['data'][i]['values']['output_type']).strip()}"
            outputtype = str(result['data'][i]['values']['output_type']).strip()
            output = bt.populateTemplateFromMap(templatestring,item["values"],outputtype,outputfilename)
            if not output["success"]:
                print(output["message"])
                exit()

            # lets print it out to stdout
            print('-------',''.join(['-' for lst in str(result['data'][i]['group_name'])]))
            print(f"output={str(result['data'][i]['group_name'])}:")
            print('-------',''.join(['-' for lst in str(result['data'][i]['group_name'])]))
            print(f"{output['data']}\n\n")

