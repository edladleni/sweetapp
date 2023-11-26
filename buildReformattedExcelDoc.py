import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil import parser
import buildTools as bt
import warnings

warnings.filterwarnings('ignore')

def reformatExcel(inputf,outputf='',datetime_fmt='%d/%m/%Y',datetime_perform=False,datetime_to_numeric=False,datetime_cols=[],decimal_fmt='%.2f',decimal_perform=False,decimal_cols=[],percentage_fmt='%.4f',percentage_perform=False,percentage_cols=[]):
    # open our file for reading
    try:
        df_sheet_list = pd.read_excel(inputf, sheet_name=None)
    except Exception as ex:
        return {"success":False,"message":f"Could not open {inputf} {ex.args}"}
    
    # create an output file name for our writer
    if len(outputf.strip()) <= 0:
        # lets create the name based upon the input name
        outputf = f"{inputf.replace(os.path.basename(inputf),'')}new_{os.path.basename(inputf)}"
    
    # create our writer object
    try:
        with pd.ExcelWriter(outputf) as writer:
            # for each sheet we do the processing
            for sheet in df_sheet_list:
                # lets create a dataframe for our sheet
                df = pd.DataFrame(df_sheet_list[sheet])
                # next we validate our data types of our columns
                for i,col in enumerate(df.columns):
                    # we only do the datetime checks if the flag has been set
                    if datetime_perform:
                        # for dates we re-format the column
                        if df.dtypes[col] == "datetime64[ns]":
                            df[col] = df[col].dt.strftime(datetime_fmt)

                    # we only do the decimal checks if the flag has been set
                    if decimal_perform:
                        # for dates we re-format the column
                        if df.dtypes[col] == "float64":
                            df[col] = df[col].apply(lambda x: decimal_fmt % x)
                    # we also force certain "decimal" columns to be re-formated
                    for dcol in decimal_cols:
                        if str(dcol).strip() == str(col).strip():
                            df[col] = pd.to_numeric(df[col],errors='coerce')
                            df[col] = df[col].fillna(0)
                            df[col] = df[col].apply(lambda x: decimal_fmt % x)
                    
                    if df.dtypes[col] == "object":
                        df[col] = df[col].astype(str)
                # next we change the columns that we explicity stated as dates
                # we can also at this stage convert the date columsn to int this functionality
                # is only available when a list of date columns are given
                if datetime_perform and len(datetime_cols)>0:
                    for row in df.index:
                        for dcol in datetime_cols:
                            dcol = str(dcol).strip()
                            try:
                                res = parser.parse(str(df[dcol][row]))
                                dtfield = str(res)
                            except:
                                dtfield = '1900-01-01 00:00:00'
                            dt = datetime.strptime(dtfield,'%Y-%m-%d %H:%M:%S')
                            if datetime_to_numeric:
                                df[dcol][row] = int(dt.strftime(datetime_fmt))
                            else:
                                # this is the normal behaviour
                                df[dcol][row] = dt.strftime(datetime_fmt)
                    for dcol in datetime_cols:
                        if datetime_to_numeric:
                            df[dcol] = pd.to_numeric(df[dcol], errors='coerce')
                        else:
                            # this is the normal behaviour
                            df[dcol] = pd.to_datetime(df[dcol], errors='coerce')
                            df[dcol] = df[dcol].dt.strftime(datetime_fmt)   
                # next we change the columns that we explicity stated as percentage
                if percentage_perform and len(percentage_cols)>0:
                    for row in df.index:
                        for pcol in percentage_cols:
                            pcol = str(pcol).strip()
                            value = str(df[pcol][row]).replace('%','')

                            if bt.checkStringDataType(value) == str:
                                newvalue = 0.00
                            elif bt.checkStringDataType(value) == int:
                                if int(value)<=100 and int(value)>=0:
                                    newvalue = int(value) / 100
                                else:
                                    newvalue = 0.00
                            elif bt.checkStringDataType(value) == float:
                                if float(value)<=1:
                                    newvalue = float(value)
                                elif float(value)<=100 and float(value)>=0:
                                    newvalue = float(value) / 100
                                else:
                                    newvalue = 0.00
                            else:
                                newvalue = 0.00
                            df[pcol][row] = newvalue
                            
                    for pcol in percentage_cols:
                        df[pcol] = pd.to_numeric(df[pcol],errors='coerce')
                        df[pcol] = df[pcol].fillna(0)
                        df[pcol] = df[pcol].apply(lambda x: percentage_fmt % x)
                        
  
                # now that we have re-formated the columns lets output the sheet
                df = df.replace({np.nan:None})
                df = df.replace({'nan':None})
                df.to_excel(writer, sheet_name=sheet, index=False)            
    except Exception as ex:
        return {"success":False,"message":f"The folowing error occurured creating {inputf} {ex.args}"}
    
    return {"success":True,"data":outputf}

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--exceldoc",type=str,help="Supply the Excel document to reformat", required=True)
    parser.add_argument("--datetimecols",type=str,help="Supply any additional columns to be reformatted (to date columns) (list each column seperated by a ,)", required=False)
    parser.add_argument("--decimalcols",type=str,help="Supply any additional columns to be reformatted (to decimal columns) (list each column seperated by a ,)", required=False)
    parser.add_argument("--percentagecols",type=str,help="Supply any additional columns to be reformatted (to percentage columns) (list each column seperated by a ,)", required=False)
    args = parser.parse_args()
    
    # lets see if we recieved a list of datetimecols
    if args.datetimecols is not None and len(str(args.datetimecols).strip())>0:
        datetime_perform = True
        datetime_cols = str(args.datetimecols).strip().split(',')
    else:
        datetime_perform = False
        datetime_cols = []    

    # lets see if we recieved a list of decimalcols
    if args.decimalcols is not None and len(str(args.decimalcols).strip())>0:
        decimal_perform = True
        decimal_cols = str(args.decimalcols).strip().split(',')
    else:
        decimal_perform = False
        decimal_cols = []

    # lets see if we recieved a list of percentagecols
    if args.percentagecols is not None and len(str(args.percentagecols).strip())>0:
        percentage_perform = True
        percentage_cols = str(args.percentagecols).strip().split(',')
    else:
        percentage_perform = False
        percentage_cols = []
        
    result = reformatExcel(args.exceldoc,datetime_perform=datetime_perform,datetime_cols=datetime_cols,decimal_perform=decimal_perform,decimal_cols=decimal_cols,percentage_perform=percentage_perform,percentage_cols=percentage_cols)
    if not result["success"]:
        print(result["message"])
