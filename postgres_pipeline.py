import requests
import json
import pandas as pd
import csv
import psycopg2

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "b7d00ef2a3mshd1ecc6521d57cbap15f3dejsncfb5ad86adb7",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

#print(response.json())

data=response.json()

#Save data into a json file
filename="PropertyRecords.json"
with open (filename,'w') as file:
    json.dump (data,file,indent=4)

    #Read into a dataframe
propertyrecord_df=pd.read_json("PropertyRecords.json")

# Transformation Layer
# Convert dictionary column to string
propertyrecord_df['features']=propertyrecord_df['features'].apply(json.dumps)

# Replacing missing values
propertyrecord_df.fillna({
    'bedrooms':0,
    'squareFootage':0,
    'yearBuilt':0,
'yearBuilt':0,
'assessorID':'Unknown',
'ownerOccupied':0,
'bathrooms':0,
'lotSize':0,
'propertyType':'Unknown',
'taxAssessment':'Not Available',
'propertyTaxes':'Not Available',
'owner':'Unknown',
'addressLine2':'Not Available',
'legalDescription':'Not Available',
'subdivision':'Not Available',
'zoning':'Unknown',
'lastSalePrice':0,
'lastSaleDate':'Not Available',
'features':'None',
'county':'Not Available'},inplace=True)

# Create the fact table
fact_columns=['addressLine1','city','state','zipCode','formattedAddress','squareFootage','yearBuilt','bathrooms','bedrooms','lotSize','propertyType','longitude','latitude']
fact_table=propertyrecord_df[fact_columns]

# Creating location Dimension
location_dim=propertyrecord_df[['addressLine1','city','state','zipCode','longitude','latitude']].drop_duplicates().reset_index(drop=True)
location_dim.index.name='location_id'

# Creating Sales Dimension
sales_dim=propertyrecord_df[['lastSalePrice','lastSaleDate']].drop_duplicates().reset_index(drop=True)
sales_dim.index.name='sales_id'

# Create property feature dimension
feature_dim=propertyrecord_df[['features','propertyType','zoning']].drop_duplicates().reset_index(drop=True)
feature_dim.index.name='feature_id'

# Loading all into a csv file
fact_table.to_csv('property_fact.csv',index=False)
location_dim.to_csv('location_dimension.csv',index=True)
sales_dim.to_csv('sales_dimension.csv',index=True)
feature_dim.to_csv('features_dimension.csv',index=True)


# Loading layer
#develop a function to connect to pgadmin
def get_db_connection():
    connection=psycopg2.connect(
    host='localhost',
    database='postgres',
    port='5432',
    user='postgres',
    password='*******'
)
    return connection

conn=get_db_connection()

# Creating database tables
def create_tables():
    conn=get_db_connection()
    cursor=conn.cursor()
    create_table_query='''
    CREATE SCHEMA IF NOT EXISTS zapbank;
    DROP TABLE IF EXISTS zipbank.fact_table;
    DROP TABLE IF EXISTS zipbank.location_dim;
    DROP TABLE IF EXISTS zipbank.sales_dim;
    DROP TABLE IF EXISTS zipbank.feature_dim;
    
    CREATE TABLE zapbank.fact_table(
    addressLine1 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode INTEGER,
    formattedAddress VARCHAR(255),
    squareFootage FLOAT,
    yearBuilt FLOAT,
    bathrooms FLOAT,
    bedrooms FLOAT,
    lotSize FLOAT,
    propertyType VARCHAR(100),
    longitude FLOAT,
    latitude FLOAT
    );
    
    CREATE TABLE zapbank.location_dim(
    location_id SERIAL PRIMARY KEY,
    addressLine1 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode INTEGER,
    longitude FLOAT,
    latitude FLOAT
    );
    
    CREATE TABLE zapbank.sales_dim(
    sales_id SERIAL PRIMARY KEY,
    lastSalePrice FLOAT,
    lastSaleDate DATE
    );
    
    CREATE TABLE zapbank.feature_dim(
    feature_id SERIAL PRIMARY KEY,
    features TEXT,
    propertyType VARCHAR(100),
    zoning VARCHAR(100)
    );
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    
create_tables()

# Create a function to load the csv data into the database
def load_data_from_csv_to_table(csv_path, table_name):
    conn=get_db_connection()
    cursor=conn.cursor()
    with open(csv_path,'r',encoding='utf-8') as file:
        reader=csv.reader(file)
        next(reader) #skip the header row
        for row in reader:
            placeholders=', '.join(['%s'] * len(row))
            query=f'INSERT INTO {table_name} VALUES ({placeholders})';
            cursor.execute(query,row)
        conn.commit()
        cursor.close()
        conn.close()
        

# loading fact table
fact_csv_path=r'C:\Users\USER\PostgresPipeline Project\property_fact.csv'
load_data_from_csv_to_table(fact_csv_path,'zapbank.fact_table')

# location dimension table
location_csv_path=r'C:\Users\USER\PostgresPipeline Project\location_dimension.csv'
load_data_from_csv_to_table(location_csv_path,'zapbank.location_dim')

#Loading features table
feature_csv_path=r'C:\Users\USER\PostgresPipeline Project\features_dimension.csv'
load_data_from_csv_to_table(feature_csv_path,'zapbank.feature_dim')

def load_data_from_csv_to_sales_table(csv_path,table_name):
    conn=get_db_connection()
    cursor=conn.cursor()
    with open(csv_path,'r',encoding='utf-8') as file:
        reader=csv.reader(file)
        next(reader) # to skip the header row
        for row in reader:
            #convert empty string(not available) in date column to none (null in sql)
            row=[None if (cell == '' or cell == 'Not Available') and col_name == 'lastSaleDate' else cell for cell,col_name in zip(row,sale_dim_column)]
            placeholders=', '.join(['%s'] * len(row))
            query=f'INSERT INTO {table_name} VALUES ({placeholders})';
            cursor.execute(query,row)
        conn.commit()
        cursor.close()
        conn.close()

#define the column name in sales_dim table
sale_dim_column=['sales_id','lastSalePrice','lastSaleDate']


# Sales dimension table
sales_csv_path=r'C:\Users\USER\PostgresPipeline Project\sales_dimension.csv'
load_data_from_csv_to_sales_table(sales_csv_path,'zapbank.sales_dim')

print ('All data has been loaded successfully into their respective schema and tables')
