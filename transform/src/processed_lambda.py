import pandas as pd
import logging, boto3, os, re, json
from datetime import datetime
import awswrangler as wr
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

INGESTION_ZONE_BUCKET = os.environ["ingestion_zone_bucket"]
PROCESSED_ZONE_BUCKET = os.environ["processed_data_zone_bucket"]
# INGESTION_ZONE_BUCKET = "de-team-heritage-ingestion-zone-20240521145550499200000002"
# PROCESSED_ZONE_BUCKET = "de-team-heritage-processed-data-zone-20240522081526993900000002"

# this function takes in an address json file and restructures it to match the dim_location table


def conversion_for_dim_location(df):
    df = df.drop(["created_at", "last_updated"], axis=1)
    df.rename(columns={"address_id": "location_id"}, inplace=True)
    df = df.convert_dtypes()

    return  df


# this function takes in a currnecy json file and restructures it to match dim_currency table


def conversion_for_dim_currency(df):
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


# this function takes in a design json file and restructures it to match dim_design table


def conversion_for_dim_design(df):
    df = df.drop(["created_at", "last_updated"], axis=1)
    df = df.convert_dtypes()
    return df


# this function takes in an address json file and counterparty json file and restructures it to match dim_counterparty table


def conversion_for_dim_counterparty(ad_df, cp_df):
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


# this function takes in a department json file and staff json file and restructures it to match the dim_staff table


def conversion_for_dim_staff(dep_df, staff_df):
    staff_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    dep_df = dep_df[["department_id", "department_name", "location"]]
    df = pd.merge(staff_df, dep_df, on="department_id", how="left")
    df = df.drop("department_id", axis=1)
    df = df.convert_dtypes()
    return df


# this function takes in dataframe (used for a date dataframe) and creates the columns needed for the dim_date table


def date_helper(date_df, column):
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


def conversion_for_dim_date(sales_order_df):
    df = sales_order_df
    created_at_df = df[["created_at"]]
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


# this function takes in a sales_order json file and restructures the data to match the fact_sales_order table


def conversion_for_fact_sales_order(sales_order_df):
    
    df = sales_order_df
    df["sales_record_id"] = [i for i in range(len(df))]
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
    

def lambda_handler(event, context):
    try:
        client = boto3.client("s3")
        if client.list_objects_v2(Bucket=PROCESSED_ZONE_BUCKET)["KeyCount"] == 0:
            ingestion_files = client.list_objects_v2(Bucket=INGESTION_ZONE_BUCKET)
            department_df = ""
            address_df = ""
            for bucket_key in ingestion_files["Contents"]:
                pattern = re.compile(r"(['/'])(\w+)")
                match = pattern.search(bucket_key["Key"])
                key_name = bucket_key["Key"]
                
                if match:
                    table_name = match.group(2)
                    logger.info(f"Processing table: {table_name}")
                    # Retrieve JSON data from S3
                    resp = client.get_object(Bucket = INGESTION_ZONE_BUCKET, Key= key_name)
                    file_content = resp['Body'].read().decode('utf-8')
                    data = json.loads(file_content)
                    
                    if table_name == "sales_order":
                        # Convert JSON data to DataFrame
                        df = pd.DataFrame(data, index=['sales_order_id'])
                        df = conversion_for_fact_sales_order(df)
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')
                        
                    elif table_name == "address":
                        address_df = pd.DataFrame(data, index=['address_id'])
                                        
                    elif table_name == "counterparty":
                        if address_df:
                            counterparty_df = pd.DataFrame(data, index=['counterparty_id'])
                            df = conversion_for_dim_counterparty(address_df, counterparty_df)
                            wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')
                    
                    elif table_name == "department":
                        department_df = pd.DataFrame(data, index=['department_id'])

                    elif table_name == "staff":
                        if department_df:
                            staff_df = pd.DataFrame(data, index=['staff_id'])
                            df = conversion_for_dim_staff(department_df, staff_df)
                            wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')
                    
                    elif table_name == "location":
                        location_df = pd.DataFrame(data, index=['address_id'])
                        df = conversion_for_dim_location(location_df)
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')

                    elif table_name == "design":
                        design_df = pd.DataFrame(data, index=['design_id'])
                        df = conversion_for_dim_design(design_df)
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')
                    
                    elif table_name == "currency":
                        currency_df = pd.DataFrame(data, index=['currency_id'])
                        df = conversion_for_dim_currency(currency_df)
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')   

                    elif table_name == "date":
                        sales_df = pd.DataFrame(data, index=['staff_id'])
                        df = conversion_for_dim_date(sales_df)
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name[:-5]}.parquet')             

                else:
                    logger.error("No match found for key: {key_name}")

    except KeyError as k:
        logger.error(f"Error retrieving data, {k}")
    except ClientError as c:
        if c.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"No object found - {key_name}")
        elif c.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"No such bucket - {PROCESSED_ZONE_BUCKET}")
        else:
            raise
    except UnicodeError:
        logger.error(f"File {key_name} is not a valid text file")
    except Exception as e:
        logger.error(e)
        raise RuntimeError


# conversion_for_fact_sales_order('load/src/sales_order-23_42_58.245848.json')


# Have an initialisation function that converts all files in the ingestion zone if the processed
# zone bucket is empty

# Thereafter, just execute for the object(s) added to the bucket (which is the trigger)
if __name__ == "__main__":
    lambda_handler('a' , 'b')