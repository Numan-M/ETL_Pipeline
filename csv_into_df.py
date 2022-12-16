import pandas as pd 
from pathlib import Path
file_path = "data/chesterfield_25-08-2021_09-00-00 - Copy.csv"

column_names = [
        "timestamp",
        "store_name",
        "customer_name",
        "products",
        "total_price",
        "payment_method",
        "card_number",
    ]


to_drop_products= ["customer_name","card_number"]
to_drop_stores =["timestamp","products","total_price","payment_method","customer_name","card_number"]
to_drop_payment=["timestamp","products","total_price","store_name","customer_name","card_number"]
to_drop_transaction = ["products","store_name","customer_name","card_number","payment_method"]

def turn_file_into_dataframe(
    file_path: str, col_names: list = column_names
) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        return dataframe
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e





def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_to_drop: list
) -> pd.DataFrame:

    """Removes specified columns from a dataframe"""

    try:
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        return dataframe
    except:
        print("dataframe does not contain specified columns")


clean_dataframe = remove_columns_from_df(turn_file_into_dataframe(file_path,column_names),to_drop_products)

def splitting_products_column(df):
    df['products'] = df['products'].str.split(', ')
    col = 'products'

    increment = 0
    for i in range(len(df[col])):
        current = df[col].iloc[i]

        # making seperate columns for price and size
        # increment = 0
        for x in current:
            # print(x.split()[1:-2])
            products_split_list = x.split()[1:-2]
            df.loc[increment:, 'size'] = x.split()[0]
            df.loc[increment:, 'product_type'] = ' '.join(products_split_list)
            df.loc[increment:, 'price'] = x.split()[-1]
            increment += 1
    return df
df_products = splitting_products_column(clean_dataframe)
pd.set_option('display.max_colwidth', None)


