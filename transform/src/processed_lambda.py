import pandas as pd
import logging, boto3, os, re, json, urllib
from datetime import datetime
import awswrangler as wr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

INGESTION_ZONE_BUCKET = os.environ["ingestion_zone_bucket"]
PROCESSED_ZONE_BUCKET = os.environ["processed_data_zone_bucket"]
department_df = ""
address_df = ""


def conversion_for_dim_location(df):
    """
    This function takes in an address dataframe and restructures it to match the dim_location table
    """
    df = df.drop(["created_at", "last_updated"], axis=1)
    df.rename(columns={"address_id": "location_id"}, inplace=True)
    df = df.convert_dtypes()

    return  df


def conversion_for_dim_currency(df):
    """
    This function takes in a currency dataframe and restructures it to match dim_currency table    
    """
    df = df.drop(["created_at", "last_updated"], axis=1)
    for i in range(len(df)):
        print(df.loc[i, "currency_code"])
        if df.loc[i, "currency_code"] == "GBP":
            df.loc[i, "currency_name"] = "Pound Sterling"
        elif df.loc[i, "currency_code"] == "USD":
            df.loc[i, "currency_name"] = "US Dollar"
        elif df.loc[i, "currency_code"] == "EUR":
            df.loc[i, "currency_name"] = "Euro"
    df = df.convert_dtypes()
    return  df





def conversion_for_dim_design(df):
    """
    This function takes in a design dataframe and restructures it to match dim_design table
    """
    df = df.drop(["created_at", "last_updated"], axis=1)
    df = df.convert_dtypes()
    return df


def conversion_for_dim_counterparty(ad_df, cp_df):
    """
    This function takes in address and counterparty dataframes and restructures them to match dim_counterparty table
    """
    ad_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    ad_df = ad_df.add_prefix("counterparty_legal_")
    ad_df.rename(
        columns={"counterparty_legal_address_id": "legal_address_id", 'counterparty_legal_phone': 'counterparty_legal_phone_number'}, inplace=True
    )

    cp_df = cp_df[["counterparty_id", "counterparty_legal_name", "legal_address_id"]]
    df = pd.merge(cp_df, ad_df, on="legal_address_id", how="left")
    df = df.drop("legal_address_id", axis=1)

    df.rename(columns={"address_id": "legal_address_id"}, inplace=True)
    df = df.convert_dtypes()
    return df


def conversion_for_dim_staff(dep_df, staff_df):
    """
    This function takes in department and staff dataframes and restructures them to match the dim_staff table
    """
    staff_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    dep_df = dep_df[["department_id", "department_name", "location"]]
    df = pd.merge(staff_df, dep_df, on="department_id", how="left")
    df = df.drop("department_id", axis=1)
    df = df.convert_dtypes()
    return df


def date_helper(date_df, column):
    """
    This function takes in a date dataframe and creates the columns needed for the dim_date table
    """
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


def conversion_for_dim_date(sales_order_df):
    """
    This function takes in a sales_order dataframe and creates dataframes from the date columns
    It calls the function above and combines all rows while removing duplicates
    The output matches the requirements of the dim_date table
    """
    df = sales_order_df.copy()
    created_at_df = df[["created_at"]]
    created_at_df.created_at = created_at_df.created_at.astype("datetime64[ns]")
    created_date_df = date_helper(created_at_df, "created_at")

    last_updated_date_df = df[["last_updated"]]
    last_updated_date_df.last_updated = last_updated_date_df.last_updated.astype(
        "datetime64[ns]"
    )
    last_updated_date_df = date_helper(last_updated_date_df, "last_updated")

    agreed_payment_date_df = df[["agreed_payment_date"]]
    agreed_payment_date_df.agreed_payment_date = (
        agreed_payment_date_df.agreed_payment_date.astype("datetime64[ns]")
    )
    agreed_payment_date_df = date_helper(agreed_payment_date_df, "agreed_payment_date")

    agreed_delivery_date_df = df[["agreed_delivery_date"]]
    agreed_delivery_date_df.agreed_delivery_date = (
        agreed_delivery_date_df.agreed_delivery_date.astype("datetime64[ns]")
    )
    agreed_delivery_date_df = date_helper(
        agreed_delivery_date_df, "agreed_delivery_date"
    )
    frames = [
        created_date_df,
        last_updated_date_df,
        agreed_payment_date_df,
        agreed_delivery_date_df,
    ]
    dim_date_df = pd.concat(frames)
    dim_date_df = dim_date_df.drop_duplicates()

    return  dim_date_df


def conversion_for_fact_sales_order(sales_order_df):
    """
    This function takes in a sales_order dataframe and restructures it to match the fact_sales_order table
    """    
    df = sales_order_df.copy()
    df.created_at = df.created_at.astype("datetime64[ns]")
    df.last_updated = df.last_updated.astype("datetime64[ns]")
    df.agreed_payment_date = df.agreed_payment_date.astype("datetime64[ns]")
    df.agreed_delivery_date = df.agreed_delivery_date.astype("datetime64[ns]")

    df['created_date'] = df['created_at'].dt.date
    df['created_time'] = df['created_at'].dt.time
    df['last_updated_date'] = df['last_updated'].dt.date
    df['last_updated_time'] = df['last_updated'].dt.time
    df['agreed_payment_date'] = df['agreed_payment_date'].dt.date
    df['agreed_delivery_date'] = df['agreed_delivery_date'].dt.date

    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

    return df


def process_file(client, key_name):
    pattern = re.compile(r"(['/'])(\w+)")
    match = pattern.search(key_name)
    table_name = match.group(2)
    global department_df, address_df
    # Retrieve JSON data from S3
    resp = client.get_object(Bucket = INGESTION_ZONE_BUCKET, Key= key_name)
    file_content = resp['Body'].read().decode('utf-8')
    data = json.loads(file_content)
        
    if "sales_order" in key_name:
        # Convert JSON data to DataFrame
        sales_df = pd.DataFrame(data)
        new_file_name = re.sub(table_name, f'fact_{table_name}', key_name)
        df = conversion_for_fact_sales_order(sales_df)
        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')

        df = conversion_for_dim_date(sales_df)
        new_file_name = re.sub(table_name, f'dim_date', key_name)
        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')

            
    elif "address" in key_name:
        df = pd.DataFrame(data)
        address_df = df.copy()
        df = conversion_for_dim_location(df)
        new_file_name = re.sub(table_name, 'dim_location', key_name)
        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')
                        
    elif "counterparty" in key_name:
        if type(address_df) != str:
            counterparty_df = pd.DataFrame(data)
            df = conversion_for_dim_counterparty(address_df, counterparty_df)
            new_file_name = re.sub(table_name, f'dim_{table_name}', key_name)
            wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')
    
    elif "department" in key_name:
        department_df = pd.DataFrame(data)

    elif "staff" in key_name:
        if type(department_df) != str:
            staff_df = pd.DataFrame(data)
            df = conversion_for_dim_staff(department_df, staff_df)
            new_file_name = re.sub(table_name, f'dim_{table_name}', key_name)
            wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')

    elif "design" in key_name:
        design_df = pd.DataFrame(data)
        df = conversion_for_dim_design(design_df)
        new_file_name = re.sub(table_name, f'dim_{table_name}', key_name)
        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')
    
    elif "currency" in key_name:
        currency_df = pd.DataFrame(data)
        df = conversion_for_dim_currency(currency_df)
        new_file_name = re.sub(table_name, f'dim_{table_name}', key_name)
        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{new_file_name[:-5]}.parquet')             

    else:
        print("No match found.")


def lambda_handler(event, context):
    
    try:
        client = boto3.client("s3")
        department_df = ""
        address_df = ""

        if client.list_objects_v2(Bucket=PROCESSED_ZONE_BUCKET)["KeyCount"] == 0:
            ingestion_files = client.list_objects_v2(Bucket=INGESTION_ZONE_BUCKET)

            for bucket_key in ingestion_files['Contents']:
                process_file(client, bucket_key["Key"])
        
        # Process only the new files added (triggered by the event)
        else:
            print("event:", event)
            s3_object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
            print("event triggered by s3 key:", s3_object_key)
            if s3_object_key[-4:] != 'json':
                logger.error(f"File is not a valid json file")
            else:
                process_file(client, s3_object_key)

    except KeyError as k:
        logger.error(f"Error retrieving data, {k}")
    except ClientError as c:
        if c.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"No such key: {c}")
        elif c.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"No such bucket: {c}")
        else:
            logger.error(f"Error InvalidClientTokenId: {c}")
    except UnicodeDecodeError as e:
        logger.error(f'Unable to decode the file: {e}')
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    