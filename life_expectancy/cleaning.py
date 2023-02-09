import argparse
import pandas as pd
from config import DATA_DIR

def parse_args():
    """Parse command line arguments"""
    parser=argparse.ArgumentParser()
    parser.add_argument("region", help="Country code to filter the data", nargs='?', const='PT')
    args=parser.parse_args()
    return args

def read_data():
    """Reads input raw table data"""
    filepath = DATA_DIR / "eu_life_expectancy_raw.tsv"
    return pd.read_table(filepath, na_values=': ')

def clean_id_column_name(df_):
    """Keeps only the text before the symbol \\ in the id column name"""
    df = df_.copy()
    df.columns = [c.strip().split("\\")[0] for c in df.columns]
    return df

def unpivot_data(df_):
    """Unpivots the data"""
    return pd.melt(df_, id_vars='unit,sex,age,geo', var_name='year', value_vars=df_.columns[1:])

def explode_data_from_column(df_, col, split):
    """Explode the concatenated information in the id column into multiple different columns"""
    df = df_.copy()
    new_cols = col.split(split)
    df[new_cols] = df[col].str.split(',', expand=True)
    return df

def rename_cols(df_):
    """Rename columns"""
    return df_.rename(columns={'geo': 'region'})

def extract_value_number(df_):
    """Clean the spaces and letters from the value column"""
    df = df_.copy()
    df['value'] = df['value'].astype(str).str.extract(r'(\d*\.\d*)', expand=False)
    return df

def change_dtypes(df_):
    """Change column types"""
    df = df_.copy()
    df['year'] = df['year'].astype(int)
    df['value'] = df['value'].astype(float)
    return df

def drop_missing_value(df_):
    """Drop rows where the value is missing"""
    return df_.dropna(subset=['value'])

def filter_region(df_, r):
    """Filter data by region"""
    if r is None:
        r = 'PT'
    return df_[df_.region==r].reset_index(drop=True)

def select_columns(df_):
    """Select output columns"""
    return df_[['unit', 'sex', 'age', 'region', 'year', 'value']]

def save_csv(df):
    """Save data to csv file"""
    filepath = DATA_DIR / "pt_life_expectancy.csv"
    df.to_csv(filepath, index=False)


def clean_data(r=None):
    """ Reads, processes and saves processed data to output file"""

    df = read_data()
    df = clean_id_column_name(df)
    df = unpivot_data(df)
    df = explode_data_from_column(df, 'unit,sex,age,geo', split=',')
    df = rename_cols(df)
    df = extract_value_number(df)
    df = change_dtypes(df)
    df = drop_missing_value(df)
    df = filter_region(df, r)
    df = select_columns(df)
    save_csv(df)

    return df


if __name__ == "__main__":  # pragma: no cover

    args = parse_args()
    df = clean_data(r=args.region)
