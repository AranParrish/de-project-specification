import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# this function takes in an address json file and restructures it to match the dim_location table

def conversion_for_dim_location(file_name):
    df = pd.read_json(file_name)
    df = df.drop(['created_at', 'last_updated'], axis = 1)
    df.rename(columns={'address_id': 'location_id'}, inplace = True)
    df = df.set_index('location_id')
    df = df.convert_dtypes()
    # df.to_parquet(f'{datetime.now().date()}/dim_location-{datetime.now().time()}.parquet')
    return ('dim_location', df)

# this function takes in a currnecy json file and restructures it to match dim_currency table

def conversion_for_dim_currency(file_name):
    df = pd.read_json(file_name)
    df = df.drop(['created_at', 'last_updated'], axis = 1)
    for i in range(len(df)):
        print(df.loc[i, 'currency_code'])
        if df.loc[i,'currency_code'] == 'GBP':
            df.loc[i, 'currency_name'] = 'Pound Sterling'
        elif df.loc[i,'currency_code'] == 'USD':
            df.loc[i, 'currency_name'] = 'US Dollar'
        elif df.loc[i,'currency_code'] == 'EUR':
            df.loc[i, 'currency_name'] = 'Euro'
    df = df.set_index('currency_id')
    return ('dim_currency', df)

# this function takes in a design json file and restructures it to match dim_design table

def conversion_for_dim_design(file_name):
    df = pd.read_json(file_name)
    df = df.drop(['created_at', 'last_updated'], axis = 1)
    df = df.set_index('design_id')
    return ('dim_design', df)

# this function takes in an address json file and counterparty json file and restructures it to match dim_counterparty table

def conversion_for_dim_counterparty(ad_file, cp_file):
    ad_df = pd.read_json(ad_file)
    ad_df.drop(['created_at', 'last_updated'], axis = 1, inplace = True)
    ad_df = ad_df.add_prefix('counterparty_legal_') 
    ad_df.rename(columns={'counterparty_legal_address_id': 'legal_address_id'}, inplace = True)

    cp_df = pd.read_json(cp_file)
    cp_df = cp_df[['counterparty_id', 'counterparty_legal_name', 'legal_address_id']]
    df = pd.merge(cp_df, ad_df, on = 'legal_address_id', how = 'left')
    df = df.drop('legal_address_id', axis = 1)

    df.rename(columns={'address_id': 'legal_address_id'}, inplace = True)
    df = df.set_index('counterparty_id')
    return ('dim_counterparty', df)

# this function takes in a department json file and staff json file and restructures it to match the dim_staff table

def conversion_for_dim_staff(dep_file, staff_file):
    staff_df = pd.read_json(staff_file)
    staff_df.drop(['created_at', 'last_updated'], axis = 1, inplace = True)
    dep_df = pd.read_json(dep_file)
    dep_df = dep_df[['department_id', 'department_name', 'location']]
    df = pd.merge(staff_df, dep_df, on = 'department_id', how = 'left')
    df = df.drop('department_id', axis = 1)
    df = df.set_index('staff_id')
    return ('dim_staff', df)

# this function takes in dataframe (used for a date dataframe) and creates the columns needed for the dim_date table

def conversion_for_dim_date_helper(date_df, column):
    df = pd.DataFrame()
    # print(type(date_df.loc[0, "created_at"]))
    for i in range(len(date_df)):
        df.loc[i,'date_id'] = date_df.loc[i, column].date()
        df.loc[i,'year'] = date_df.loc[i, column].year
        df.loc[i, 'month'] = date_df.loc[i, column].month
        df.loc[i, 'day'] = date_df.loc[i, column].day
        df.loc[i, 'day_of_week'] = date_df.loc[i, column].dayofweek
        df.loc[i, 'day_name'] = date_df.loc[i, column].day_name()
        df.loc[i, 'month_name'] = date_df.loc[i, column].month_name()
        df.loc[i, 'quarter'] = date_df.loc[i, column].quarter
    

    df.year = df.year.astype('int64')
    df.month = df.month.astype('int64')
    df.day = df.day.astype('int64')
    df.day_of_week = df.day_of_week.astype('int64')
    df.quarter = df.quarter.astype('int64')
    #print(df.dtypes)
    #print(df.head())
    
    return df

# this function takes in a sales_order json file and creates dataframes from the date columns
# it calls the function above and combines all rows while removing duplicates 
# the output matches the requirements of the dim_date table

def dim_date_tb(sales_order_file):
    df = pd.read_json(sales_order_file)
    created_at_df = df[['created_at']]
    #created_at_df = created_at_df.drop_duplicates()
    #print(created_at_df.shape)
    created_date_df = conversion_for_dim_date_helper(created_at_df, "created_at")
    
    last_updated_date_df = df[['last_updated']]
    #print(last_updated_date_df.shape)
    last_updated_date_df.last_updated = last_updated_date_df.last_updated.astype('datetime64[ns]')
    last_updated_date_df = conversion_for_dim_date_helper(last_updated_date_df, "last_updated")

    
    agreed_payment_date_df = df[['agreed_payment_date']]
    agreed_payment_date_df.agreed_payment_date = agreed_payment_date_df.agreed_payment_date.astype('datetime64[ns]')
    agreed_payment_date_df = conversion_for_dim_date_helper(agreed_payment_date_df, "agreed_payment_date")
    
    agreed_delivery_date_df = df[['agreed_delivery_date']]
    agreed_delivery_date_df.agreed_delivery_date = agreed_delivery_date_df.agreed_delivery_date.astype('datetime64[ns]')
    agreed_delivery_date_df = conversion_for_dim_date_helper(agreed_delivery_date_df, "agreed_delivery_date")
    
    frames = [created_date_df, last_updated_date_df, agreed_payment_date_df, agreed_delivery_date_df]
    dim_date_df = pd.concat(frames)
    dim_date_df =dim_date_df.drop_duplicates()
    
    # print(dim_date_df.shape)
    return ('dim_date', dim_date_df)

# this function takes in a sales_order json file and restructures the data to match the fact_sales_order table
    
def conversion_for_fact_sales_order(sales_order_file):
    df = pd.read_json(sales_order_file)
    df['sales_record_id'] = df.index
    df.last_updated = df.last_updated.astype('datetime64[ns]')
    df.agreed_payment_date = df.agreed_payment_date.astype('datetime64[ns]')
    df.agreed_delivery_date = df.agreed_delivery_date.astype('datetime64[ns]')
    for i in df.index:
        df['created_date'] = df.loc[i,'created_at'].date()
        df['created_time'] = df.loc[i, 'created_at'].time()
        df['last_updated_date'] = df.loc[i, 'last_updated'].date()
        df['last_updated_time'] = df.loc[i, 'last_updated'].time()
        df.loc[i, 'agreed_payment_date'] = df.loc[i, 'agreed_payment_date'].date()
        df.loc[i, 'agreed_delivery_date'] = df.loc[i, 'agreed_delivery_date'].date()
    
    df.drop(['created_at', 'last_updated'], axis = 1, inplace = True)
    df.rename(columns={'staff_id': 'sales_staff_id'}, inplace = True)
    df = df.set_index('sales_record_id')
    # print(df.dtypes)
    # print(df.columns)
    return ('fact_sales_order', df)
    

def lambda_handler(event, context):
    pass
    
# conversion_for_fact_sales_order('load/src/sales_order-23_42_58.245848.json') 


# Have an initialisation function that converts all files in the ingestion zone if the processed
# zone bucket is empty

# Thereafter, just execute for the object(s) added to the bucket (which is the trigger)