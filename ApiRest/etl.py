import pandas as pd
import numpy as np
import pyodbc
import numpy as np
from datetime import datetime
from db_connect import pwds
import pyodbc

def sql_connection():

    driver = '{ODBC Driver 18 for SQL Server}'  
    server = pwds["server"]
    database = pwds["database"]         
    username = pwds["username"]    
    password = pwds["password"]             

    connection_string = f"""
    DRIVER={driver};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
    """
    try:
        connection = pyodbc.connect(connection_string)
        print("success")
        return connection
    except Exception as e:
        print(f"fail: {e}")
        return None

def clean_data(csv_file_path):

    df = pd.read_csv(csv_file_path)

    df = df.replace(to_replace="-",value=np.nan)
    df = df.replace(to_replace="",value=np.nan)
    df = df.where(pd.notnull(df), None)
    df = df.replace({np.nan: None})
    object_cols = df.select_dtypes(include='object').columns  
    df[object_cols] = df[object_cols].astype(str)
    return df
 
def etlwildcard(df,execSP="SET NOCOUNT ON",dlt=0):
    cnxn1 = sql_connection()
    cursor1 = cnxn1.cursor()
    ColumnsNumber = len(df.columns)
    placeholders = ','.join(['?'] * ColumnsNumber )
    insertcolumns = ','.join(f'[{i}]' for i in range(1,ColumnsNumber+1))
    df_insert1 = df.values.tolist()
    sql_insert_data = f"""
                        INSERT INTO [staging].[wildcard]
                        (
                        {insertcolumns}
                        )
                        VALUES
                        ({placeholders})
                        """
    try:
        t1 = datetime.now()
        print('StartEtl' ,t1)
        if dlt == 0:
            excecdlt = """
                    delete [staging].[wildcard]
                       """  ##SET NOCOUNT ON    delete [dbo].[tbETLWildcard]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            cursor1.execute(excecdlt)
            cnxn1.commit()
            print('delete OK')
        cursor1.fast_executemany = True
        cursor1.executemany(sql_insert_data,df_insert1)
        cnxn1.commit()
        t2 = datetime.now()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
        cursor1.execute(execSP)
        cnxn1.commit()
        print('EndEtl ',t2)  
        print('EtlDuration: ',(t2 - t1) )
    except (KeyError, ValueError) as e:
        print(f'Oops!, something went wrong: {str(e)}')



# csv_file_path = "C:\\Users\\DELL\\Documents\\Jhovanny\\Challenge_20240912\\data_challenge_files\\hired_employees.csv"

# # df = clean_data(csv_file_path)
# # etlwildcard(df,execSP="SET NOCOUNT ON",dlt=0)