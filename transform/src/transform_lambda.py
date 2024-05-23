import pandas as pd
import logging, boto3, os, re
from datetime import datetime
import awswrangler as wr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# INGESTION_ZONE_BUCKET = os.environ["ingestion_zone_bucket"]
# PROCESSED_ZONE_BUCKET = os.environ["processed_data_zone_bucket"]
INGESTION_ZONE_BUCKET = "de-team-heritage-ingestion-zone-20240521145550499200000002"
PROCESSED_ZONE_BUCKET = "de-team-heritage-processed-data-zone-20240522081526993900000002"

# this function takes in an address json file and restructures it to match the dim_location table


def conversion_for_dim_location(file_name):
    df = pd.read_json(file_name)
    df = df.drop(["created_at", "last_updated"], axis=1)
    df.rename(columns={"address_id": "location_id"}, inplace=True)
    df = df.set_index("location_id")
    df = df.convert_dtypes()

    return  df


# this function takes in a currnecy json file and restructures it to match dim_currency table


def conversion_for_dim_currency(file_name):
    df = pd.read_json(file_name)
    df = df.drop(["created_at", "last_updated"], axis=1)
    for i in range(len(df)):
        print(df.loc[i, "currency_code"])
        if df.loc[i, "currency_code"] == "GBP":
            df.loc[i, "currency_name"] = "Pound Sterling"
        elif df.loc[i, "currency_code"] == "USD":
            df.loc[i, "currency_name"] = "US Dollar"
        elif df.loc[i, "currency_code"] == "EUR":
            df.loc[i, "currency_name"] = "Euro"
    df = df.set_index("currency_id")
    df = df.convert_dtypes()
    return  df


# this function takes in a design json file and restructures it to match dim_design table

def conversion_for_dim_design(file_name):
    df = pd.read_json(file_name)
    df = df.drop(["created_at", "last_updated"], axis=1)
    df = df.set_index("design_id")
    df = df.convert_dtypes()
    return df


# this function takes in an address json file and counterparty json file and restructures it to match dim_counterparty table

def conversion_for_dim_counterparty(ad_file, cp_file):
    ad_df = pd.read_json(ad_file)
    ad_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    ad_df = ad_df.add_prefix("counterparty_legal_")
    ad_df.rename(
        columns={"counterparty_legal_address_id": "legal_address_id", 'counterparty_legal_phone': 'counterparty_legal_phone_number'}, inplace=True
    )
    cp_df = pd.read_json(cp_file)
    cp_df = cp_df[["counterparty_id", "counterparty_legal_name", "legal_address_id"]]
    df = pd.merge(cp_df, ad_df, on="legal_address_id", how="left")
    df = df.drop("legal_address_id", axis=1)

    df.rename(columns={"address_id": "legal_address_id"}, inplace=True)
    df = df.set_index("counterparty_id")
    df = df.convert_dtypes()
    return df


# this function takes in a department json file and staff json file and restructures it to match the dim_staff table

def conversion_for_dim_staff(dep_file, staff_file):
    staff_df = pd.read_json(staff_file)
    staff_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    dep_df = pd.read_json(dep_file)
    dep_df = dep_df[["department_id", "department_name", "location"]]
    df = pd.merge(staff_df, dep_df, on="department_id", how="left")
    df = df.drop("department_id", axis=1)
    df = df.set_index("staff_id")
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


def conversion_for_dim_date(sales_order_file):
    df = pd.read_json(sales_order_file)
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
    dim_date_df = dim_date_df.set_index('date_id')

    return  dim_date_df


# this function takes in a sales_order json file and restructures the data to match the fact_sales_order table


def conversion_for_fact_sales_order(sales_order_file):
    df = wr.s3.read_json([sales_order_file])
    df["sales_record_id"] = df.index
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
    df = df.set_index("sales_record_id")

    return df

def put_parquet_into_bucket(client, body, key):
    response = client.put_object(
                                    Body= body,
                                    Bucket=PROCESSED_ZONE_BUCKET,
                                    Key=key,
                                )
    return response
    

def lambda_handler(event, context):
    client = boto3.client("s3")
    if client.list_objects_v2(Bucket=PROCESSED_ZONE_BUCKET)["KeyCount"] == 0:
        ingestion_files = client.list_objects_v2(Bucket=INGESTION_ZONE_BUCKET)
        department_key = ""
        address_key = ""
        for bucket_key in ingestion_files["Contents"]:
            #print(bucket_key['Key'][11:])
            pattern = re.compile(r"(['/'])(\w+)")
            match = pattern.search(bucket_key["Key"])
            key_name = bucket_key["Key"]
            
            if match:
                table_name = match.group(2)

                if table_name == "sales_order":
                    df = conversion_for_fact_sales_order(f"s3://{INGESTION_ZONE_BUCKET}/{key_name}")
                    #df = conversion_for_fact_sales_order(bucket_key['Key'][11:])
                    #df = conversion_for_fact_sales_order(bucket_key['Key'])
                    wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name}.parquet')
                    put_parquet_into_bucket(client, f"transform/src/{key_name}.parquet", f"{key_name}.parquet")
                    
                elif table_name == "address":
                    address_key = f"s3://{INGESTION_ZONE_BUCKET}/{key_name}"
                
                elif table_name == "counterparty":
                    if address_key:
                        df = conversion_for_dim_counterparty(
                            address_key, f"s3://{INGESTION_ZONE_BUCKET}/{key_name}"
                        )
                        #df.to_parquet(f"{key_name}.parquet")
                        wr.s3.to_parquet(df=df, path=f's3://{PROCESSED_ZONE_BUCKET}/{key_name}.parquet')
                        #put_parquet_into_bucket(client, f"transform/src/{key_name}.parquet", f"{key_name}.parquet")
                
                elif table_name == "department":
                    department_key = f"s3://{INGESTION_ZONE_BUCKET}/{key_name}"
                elif table_name == "staff":
                    if department_key:
                        df = conversion_for_dim_staff(department_key, f"s3://{INGESTION_ZONE_BUCKET}/{key_name}")
                        df.to_parquet(f"{key_name}.parquet")
                        #put_parquet_into_bucket(client, f"transform/src/{key_name}.parquet", f"{key_name}.parquet")
                
                elif table_name == "":
                    df = function_name(f"s3://{INGESTION_ZONE_BUCKET}/{key_name}")
                    df.to_parquet(f"{key_name}.parquet")
                    #put_parquet_into_bucket(client, f"transform/src/{key_name}.parquet", f"{key_name}.parquet")


            else:
                print("No match found.")


# conversion_for_fact_sales_order('load/src/sales_order-23_42_58.245848.json')


# Have an initialisation function that converts all files in the ingestion zone if the processed
# zone bucket is empty

# Thereafter, just execute for the object(s) added to the bucket (which is the trigger)
if __name__ == "__main__":
    lambda_handler('a' , 'b')