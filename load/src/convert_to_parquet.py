import pandas as pd
# import pyarrow 
# import pyarrow.parquet
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# this function that takes in the table name and df that is structured to match schema and writes it into parquet format
# new parquet file is named with timestamp and table reference

# def convert_to_parquet(table, df):
#     arrow_table = pyarrow.Table.from_pandas(df)
#     new_file_name = f'{datetime.now().date()}/{table}-{datetime.now().time()}.parquet'
#     with open(new_file_name, 'w') as f:
#         pyarrow.parquet.write_table(arrow_table, f)
#         print(pd.read_parquet(f))
#         return f
    # else:
    #     logger.error('Attempt to convert a non json file to parquet format.')
    #     return 'This is not a json file.'

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
        if df.loc[i,'currency_code'] == 'GBP':
            df.loc[i, 'currency_name'] = 'Pound Sterling'
        elif df.loc[i,'currency_code'] == 'USD':
            df.loc[i, 'currency_name'] = 'US Dollar'
        elif df.loc[i,'currency_code'] == 'EUR':
            df.loc[i, 'currency_name'] = 'Euro'
    df = df.set_index('currency_id')
    df = df.convert_dtypes()
    return ('dim_currency', df)

# this function takes in a design json file and restructures it to match dim_design table

def conversion_for_dim_design(file_name):
    df = pd.read_json(file_name)
    df = df.drop(['created_at', 'last_updated'], axis = 1)
    df = df.set_index('design_id')
    df = df.convert_dtypes()
    return ('dim_design', df)

# this function takes in an address json file and counterparty json file and restructures it to match dim_counterparty table

def conversion_for_dim_counterparty(ad_file, cp_file):
    ad_df = pd.read_json(ad_file)
    ad_df.drop(['created_at', 'last_updated'], axis = 1, inplace = True)
    ad_df = ad_df.add_prefix('counterparty_legal_') 
    ad_df.rename(columns={'counterparty_legal_phone': 'counterparty_legal_phone_number'}, inplace = True)
    ad_df.rename(columns={'counterparty_legal_address_id': 'legal_address_id'}, inplace = True)

    cp_df = pd.read_json(cp_file)
    cp_df = cp_df[['counterparty_id', 'counterparty_legal_name', 'legal_address_id']]
    df = pd.merge(cp_df, ad_df, on = 'legal_address_id', how = 'left')
    df = df.drop('legal_address_id', axis = 1)

    df.rename(columns={'address_id': 'legal_address_id'}, inplace = True)
    df = df.set_index('counterparty_id')
    df = df.convert_dtypes()
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
    df = df.convert_dtypes()
    return ('dim_staff', df)

# this function takes in dataframe (used for a date dataframe) and creates the columns needed for the dim_date table

def conversion_for_dim_date_helper(date_df, column):
    date_df['date_id'] = date_df[column].dt.date
    date_df['year'] = date_df[column].dt.year
    date_df['month'] = date_df[column].dt.month
    date_df['day'] = date_df[column].dt.day
    date_df['day_of_week'] = date_df[column].dt.dayofweek
    date_df['day_name'] = date_df[column].dt.day_name()
    date_df['month_name'] = date_df[column].dt.month_name()
    date_df['quarter'] = date_df[column].dt.quarter

    date_df = date_df.drop(column, axis = 1)
    date_df = date_df.convert_dtypes()
    
    return date_df

# this function takes in a sales_order json file and creates dataframes from the date columns
# it calls the function above and combines all rows while removing duplicates 
# the output matches the requirements of the dim_date table

def dim_date_tb(sales_order_file):
    df = pd.read_json(sales_order_file)
    created_at_df = df[['created_at']]
    created_date_df = conversion_for_dim_date_helper(created_at_df, "created_at")
    
    last_updated_date_df = df[['last_updated']]
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
    dim_date_df = dim_date_df.set_index('date_id')

    
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
    

   
    
dim_date_tb('load/tests/data/sales_order.json') 