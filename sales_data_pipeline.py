import logging.config
import pandas as pd
import logging
import os

## configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_sales_data(filepath: str) -> pd.DataFrame:
    """"
    Loads sales data from csv file into pandas DataFrame.
    Includes Error handling for file not found. 
    """
    if not os.path.exists(filepath):
        logging.error(f"Input file Not found at file path {filepath}")
        return pd.DataFrame()   ## returning emty DataFrame on error.
    
    try:
        df = pd.read_csv(filepath)
        logging.info(f"File is Successfully Loaded {len(df)} rows from {filepath}.")
        return df
    except Exception as e:
        logging.error(f"Failed to load data from {filepath}: {e}",exc_info=True)
        return pd.DataFrame()  ## returning emty DataFrame on error.
    
def clean_and_transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    cleans and transforms the sales Dataframe:
    converts 'price' and 'quantity' to numeric.
    fills missing values with 0.
    calculate total sale '0'
    standardizes category (strip whitespace, lower case)
    Filters out invalid rows or sales with total_sale < 100.
    """
    if df.empty:
        logging.warning(f"Input Dataframe for cleaning is empty")
        return pd.DataFrame
    cleaned_df = df.copy() ## not changing the origional data (taking copy of it)

    # convert price and quantity to  numeric , correcting errors to NAN
    ##then filling the nan with 0 for quantitties and drop rows with none prices
    logging.info("---converting price and quanity to numeric---")
    cleaned_df['price'] = pd.to_numeric(df['price'],errors='coerce')
    cleaned_df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    ## Log rows that had non - numeric values converted to NAN.
    initial_invalid_prices = cleaned_df['price'].isna().sum()
    initial_invalid_quanties = cleaned_df['quantity'].isna().sum()

    if initial_invalid_prices > 0:
        logging.warning(f"Identified {initial_invalid_prices} rows with non numeric prices values")
    if initial_invalid_quanties > 0:
        logging.warning(f"Identified {initial_invalid_quanties} rows with non numeric quantity values")
    
    ## filling missing values in quantity (NAN with zero values) with 0
    cleaned_df['quantity'].fillna(0, inplace=True)

    ## Drop rows where 'price' is NAN (after coersionb) as they are un recoverable for calculations
    rows_before_price_drop = len(cleaned_df)
    cleaned_df.dropna(subset=['price'], inplace=True)
    rows_after_price_drop = len(cleaned_df)
    if rows_before_price_drop > rows_after_price_drop:
        logging.warning(f"Dropped {rows_before_price_drop - rows_after_price_drop} rows due to invalid 'price'")
    
    # calculate total sales 
    logging.info("Calculating --Total Sales--")
    cleaned_df['total_sales'] = cleaned_df['price'] * cleaned_df['quantity']

    # standardize 'category'
    logging.info("standardizing 'category' column' ")
    cleaned_df['category'] = cleaned_df['category'].astype(str).str.strip().str.lower()

    ## filter out sales with total_sale < 100 (example business rule)
    rows_before_filter = len(cleaned_df)
    cleaned_df = cleaned_df[cleaned_df['total_sales'] > 100]
    rows_after_filter = len(cleaned_df)
    if rows_before_filter > rows_after_filter:
        logging.info(f" Filtered out {rows_before_filter - rows_after_filter} sales with total sale less than 100")

    # filling missing customer email with unknow
    initial_missing_emails = cleaned_df['customer_email'].isnull().sum()
    if initial_missing_emails > 0:
        cleaned_df['customer_email'].fillna('Unknown', inplace = True)
        logging.info(f" Filled {initial_missing_emails} missing 'cutomer email' values with 'unknown' ")

    logging.info("Data Cleaning and transformation is complete")
    return cleaned_df

def summarize_sales_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates Total sales by category
    """
    if df.empty():
        logging.warning(f"input dataframe for summarization is empty")
        return pd.DataFrame()
    
    logging.info("--summarizing sales by category--")
    summary_df = df.groupby('category')['total_sale'].sum().reset_index()
    summary_df.rename(columns={'total_sale':'total_revenue'}, inplace=True)
    logging.info("Sales summarization complete")
    return summary_df

def save_dataframe_to_csv(df: pd.DataFrame, filepath: str) -> None:
    """ It saves DataFrame to a CSV"""
    try:
        df.to_csv(filepath,index=False,encoding='utf-8')
        logging.info(f"DataFrame Successfully saved to csv at {filepath}")
    except Exception as e:
        logging.error(f"Failed to DataFrame to CSV at {filepath} : {e}",exc_info='True')

def save_dataframe_to_json(df:pd.DataFrame, filepath: str) -> None:
    """ Saves DataFrame to JSOn file (orient = records for list of objects )"""
    try:
        df.to_json(filepath, orient='records', indent=2, ensure_ascii = False)
        logging.info(f"Dataframe successfully saved to {filepath}.")
    except Exception as e:
        logging.error(f"Failed to save dataframe to json at {filepath}: {e}",exc_info=True)


## main pipline Excecution
if __name__ == "__main__":
    input_csv_path = r"C:\Users\subramani.v\Documents\Data_engineering_flow\Projects\sales_data.csv"
    cleaned_output_csv_path = r"C:\Users\subramani.v\Documents\Data_engineering_flow\Projects\cleaned_sales_data.csv"
    category_summary_json_path = r"C:\Users\subramani.v\Documents\Data_engineering_flow\Projects\cleaned_sales_data.csv"

    logging.info("--Starting Sales Data ETL Pipeline--")

    # 1. Extract
    sales_df = load_sales_data(input_csv_path)
    if sales_df.empty:
        logging.error(f" Pipeline Aborted due to No data or Loading data")
    else:
        # 2. Transform
        cleaned_sales_df = clean_and_transform_sales_data(sales_df)
        if not cleaned_sales_df.empty:
            # 3. Load cleaned Data
            save_dataframe_to_csv(cleaned_sales_df,cleaned_output_csv_path)
        else:
            logging.warning(f"No Valid Data after cleaning and Transformation. skipping output genration.")
        
    logging.info(" Sales data ETL Pipeline Finished.")

    # clean up input file after excecution for re-running tests
    if os.path.exists(input_csv_path):
        os.remove(input_csv_path)