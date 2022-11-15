##############################################################################
# Import necessary modules and files
# #############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from constants import *
from city_tier_mapping import city_tier_mapping
from significant_categorical_level import *

# ignore the warnings in the output
import warnings
warnings.filterwarnings("ignore")


def load_data(file_path):
    if 'test' in file_path:
        return pd.read_csv(file_path)
    return pd.read_csv(file_path,index_col=[0])


def check_if_table_has_value(cnx, table_name):
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';", cnx).shape[0]
    if check_table == 1:
        return True
    else:
        return False
###############################################################################
# Define the function to build database
# ##############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        db_file_name : Name of the database file 'utils_output.db'
        db_path : path where the db file should be '   


    OUTPUT
    The function returns the following under the conditions:
        1. If the file exsists at the specified path
                prints 'DB Already Exsists' and returns 'DB Exsists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    if os.path.isfile(DB_PATH+DB_FILE_NAME):
        print( "DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            
            conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
            print("New DB Created")
            return 'DB created'
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"

###############################################################################
# Define function to load the csv file to the database
# ##############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in datadirectiry into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' with 0.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        data_directory : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    try:
        cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
        if not check_if_table_has_value(cnx,'loaded_data'):
            print("Table Doesn't Exsist - loaded_data, Building")
            loaded_data = load_data(f"{DATA_DIRECTORY}")
            loaded_data['total_leads_droppped'] = loaded_data['total_leads_droppped'].fillna(0)
            loaded_data['referred_lead'] = loaded_data['referred_lead'].fillna(0)
            loaded_data.to_sql(name='loaded_data', con=cnx,if_exists='replace',index=False)
            print("Writing to DataBase Done or Data Already was in Table. Check Logs.")
        else:
            print("loaded_data is already present")
    except Exception as e:
        print (f'Exception thrown in load_data_into_db : {e}')
    finally:
        if cnx:        
            cnx.close()
    

###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in /mappings/city_tier_mapping.py file. If a
    particular city's tier isn't mapped in the city_tier_mapping.py then
    the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    try:
        cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
        if not check_if_table_has_value(cnx,'city_tier_mapped'):
            print("Loaded_data Exists")
            df_lead_scoring = pd.read_sql('select * from loaded_data', cnx)
            print("Mapping city_mapped to tiers")
            df_lead_scoring["city_tier"] = df_lead_scoring["city_mapped"].map(city_tier_mapping)
            df_lead_scoring["city_tier"] = df_lead_scoring["city_tier"].fillna(3.0)
            df_lead_scoring = df_lead_scoring.drop(["city_mapped"],axis=1)
            df_lead_scoring.to_sql(name='city_tier_mapped', con=cnx,if_exists='replace',index=False)
            print("city_tier_mapped created/replaced")
        else:
            print("city_tier_mapped already exists")
    except Exception as e:
        print (f'Exception thrown in map_city_tier : {e}')
    finally:
        if cnx:        
            cnx.close()
    

###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars():
    '''
    This function maps all the unsugnificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    try:
        cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
        if not check_if_table_has_value(cnx,'categorical_variables_mapped'):
            print("city_tier_mapped Exists")
            df_lead_scoring = pd.read_sql('select * from city_tier_mapped', cnx)
            print("Mapping categorical_vars")
            
            # all the levels below 90 percentage are assgined to a single level called others
            new_df = df_lead_scoring[~df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are not present in list_platform
            new_df['first_platform_c'] = "others" # replace the value of these levels to others
            old_df = df_lead_scoring[df_lead_scoring['first_platform_c'].isin(list_platform)] # get rows for levels which are present in list_platform
            df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
            
            
            # all the levels below 90 percentage are assgined to a single level called others
            new_df = df[~df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are not present in list_medium
            new_df['first_utm_medium_c'] = "others" # replace the value of these levels to others
            old_df = df[df['first_utm_medium_c'].isin(list_medium)] # get rows for levels which are present in list_medium
            df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
            
            
            # all the levels below 90 percentage are assgined to a single level called others
            new_df = df[~df['first_utm_source_c'].isin(list_source)] # get rows for levels which are not present in list_source
            new_df['first_utm_source_c'] = "others" # replace the value of these levels to others
            old_df = df[df['first_utm_source_c'].isin(list_source)] # get rows for levels which are present in list_source
            df = pd.concat([new_df, old_df]) # concatenate new_df and old_df to get the final dataframe
            
            
            df = df.drop_duplicates()
            df.to_sql(name='categorical_variables_mapped', con=cnx,if_exists='replace',index=False)
            print("categorical_variables_mapped created/replaced")
        else:
            print("categorical_variables_mapped already exists")
    except Exception as e:
        print (f'Exception thrown in map_categorical_vars : {e}')
    finally:
        if cnx:        
            cnx.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        interaction_mapping_file : path to the csv file containing interaction's
                                   mappings
        index_columns : list of columns to be used as index while pivoting and
                        unpivoting
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our index_columns. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' in it as index_column else pass a list without 'app_complete_flag'
        in it.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    try:
        cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
        if not check_if_table_has_value(cnx, 'interactions_mapped') or not check_if_table_has_value(cnx, 'model_input'):
            print("categorical_variables_mapped exists")
            df = pd.read_sql('select * from categorical_variables_mapped', cnx)        

            # read the interaction mapping file
            print('read the interaction_mapping_file from the path')
            df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])
            
            
            print("Mapping interactions")
            
            # unpivot the interaction columns and put the values in rows
            id_vars = INDEX_COLUMNS
            if 'app_complete_flag' not in df.columns:
                id_vars.remove('app_complete_flag')

            df_unpivot = pd.melt(df, id_vars=id_vars, var_name='interaction_type', value_name='interaction_value')    

            # handle the nulls in the interaction value column
            df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)

            # map interaction type column with the mapping file to get interaction mapping
            df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')

            #dropping the interaction type column as it is not needed
            df = df.drop(['interaction_type'], axis=1)        

            # pivoting the interaction mapping column values to individual columns in the dataset
            df_pivot = df.pivot_table(
                    values='interaction_value', index=id_vars, columns='interaction_mapping', aggfunc='sum')
            df_pivot = df_pivot.reset_index()

            df_pivot.to_sql(name='interactions_mapped', con=cnx, if_exists='replace', index=False)
            print("interactions_mapped created/replaced")

            print("Selecting required columns for model traning part")
            # these columns were derived after rapid expermentation where we excluded columns with relatively low significance
            dataset_trimmed = df_pivot[INDEX_COLUMNS[1:]]   
            dataset_trimmed.to_sql(name='model_input', con=cnx, if_exists='replace', index=False)
            print("model_input created/replaced")
        else:
            print("model_input already exists")
    except Exception as e:
        print (f'Exception thrown in load_data_into_db : {e}')
    finally:
        if cnx:        
            cnx.close()
