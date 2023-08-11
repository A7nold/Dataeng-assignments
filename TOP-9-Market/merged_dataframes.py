import mysql.connector as conn
import pandas as pd

#connect to db in mysql
def initiate_database():
    mydb = conn.Connect(host = "localhost", port="3306", user = "root", passwd = "root")
    cursor = mydb.cursor()
    cursor.execute("use top20stock")
    cursor.execute("show tables")
    tables = cursor.fetchall()
    
    
    return tables


#convert sql table to dataframe
def sql_table_to_dataframe(table_name:str):
    mydb = conn.Connect(host = "localhost", port="3306", user = "root", passwd = "root")
    cursor = mydb.cursor()
    # columns = ["date","open","high","low","close","adj close","volume"]
    cursor.execute("use top20stock")
    cursor.execute(f"select * from {table_name}")
    table_convertedto_dataframe = cursor.fetchall()
    column_names = ["date","open","high","low","close","adj close","volume"]
    df = pd.DataFrame(table_convertedto_dataframe,columns=column_names)
    #cursor.close
    
    return df




def merged_dfs(open:str):
    tables = initiate_database()
    Table_list_updated = [item[0] for item in tables]
    list_of_dfs = {}
    for name in Table_list_updated:
        result = sql_table_to_dataframe(name)  # function() returns a DataFrame
        list_of_dfs[f"df_{name}"] = result 
    columns_to_select = ['date', open]  # Columns to select in the final joined DataFrame
    joined_df = list_of_dfs[list(list_of_dfs.keys())[0]][columns_to_select]
    
    #Iteratively merge with the remaining DataFrames
    for key in list(list_of_dfs.keys())[1:]:
        #joined_df['date'] = pd.to_datetime(joined_df['date'],format='%d/%m/%Y') 
        joined_df = pd.merge(joined_df, list_of_dfs[key][columns_to_select], on='date', how='outer',suffixes=('', f'_{key}'))
       
        joined_df.sort_values(by='date',ascending=True,inplace=True)
    
    #rename the first column name
    renamed_df = joined_df.rename(columns={f'{open}': f'{open}_asianpaint_ns'})
    #convert string date to datetime to sort tit in ascending order
    renamed_df['date'] = pd.to_datetime(renamed_df['date'],format='%m/%d/%Y') 
    renamed_df.sort_values(by='date',ascending=True,inplace=True)
    return renamed_df









#rough: 
#print(Table_list_updated)

# for name in Table_list_updated:
#     result = sql_table_to_dataframe(name)  # function() returns a DataFrame
#     list_of_dfs[f"df_{name}"] = result        


#merging dataframes depending on the column value either open,close,volume.    
#open_df = merged_dfs('open')
# open_df['date'] = pd.to_datetime(open_df['date'],format='%m/%d/%Y') 
# open_df.sort_values(by='date',ascending=True,inplace=True)
# closed_df = merged_dfs('close')
# volume_df = merged_dfs('volume')

#print(open_df)

