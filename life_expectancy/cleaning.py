import argparse
import pandas as pd
from life_expectancy.config import DATA_DIR

def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser=argparse.ArgumentParser()
    parser.add_argument("region", help="Country code to filter the data", nargs='?', const='PT')
    args=parser.parse_args()
    return args

def load_data() -> pd.DataFrame:
    """Reads input raw table data"""
    filepath = DATA_DIR / "eu_life_expectancy_raw.tsv"
    return pd.read_table(filepath, na_values=': ')

def clean_id_column_name(df_: pd.DataFrame) -> pd.DataFrame:
    """Keeps only the text before the symbol \\ in the id column name"""
    df = df_.copy()
    df.columns = [c.strip().split("\\")[0] for c in df.columns]
    return df

def unpivot_data(df_: pd.DataFrame) -> pd.DataFrame:
    """Unpivots the data"""
    return pd.melt(df_, id_vars='unit,sex,age,geo', var_name='year', value_vars=df_.columns[1:])

def explode_data_from_column(df_: pd.DataFrame, col: str, split: str) -> pd.DataFrame:
    """Explode the concatenated information in the id column into multiple different columns"""
    df = df_.copy()
    new_cols = col.split(split)
    df[new_cols] = df[col].str.split(',', expand=True)
    return df

def rename_cols(df_: pd.DataFrame) -> pd.DataFrame:
    """Rename columns"""
    return df_.rename(columns={'geo': 'region'})

def extract_value_number(df_: pd.DataFrame) -> pd.DataFrame:
    """Clean the spaces and letters from the value column"""
    df = df_.copy()
    df['value'] = df['value'].astype(str).str.extract(r'(\d*\.\d*)', expand=False)
    return df

def change_dtypes(df_: pd.DataFrame) -> pd.DataFrame:
    """Change column types"""
    df = df_.copy()
    df['year'] = df['year'].astype(int)
    df['value'] = df['value'].astype(float)
    return df

def drop_missing_value(df_: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where the value is missing"""
    return df_.dropna(subset=['value'])

def filter_region(df_: pd.DataFrame, r: str) -> pd.DataFrame:
    """Filter data by region"""
    if r is None:
        r = 'PT'
    return df_[df_.region==r].reset_index(drop=True)

def select_columns(df_: pd.DataFrame) -> pd.DataFrame:
    """Select output columns"""
    return df_[['unit', 'sex', 'age', 'region', 'year', 'value']]

def save_data(df: pd.DataFrame) -> None:
    """Save data to csv file"""
    filepath = DATA_DIR / "pt_life_expectancy.csv"
    df.to_csv(filepath, index=False)


def clean_data(df: pd.DataFrame, r=None) -> pd.DataFrame:
    """ Reads, processes and saves processed data to output file"""

    df_ = df.copy()
    df_ = clean_id_column_name(df_)
    df_ = unpivot_data(df_)
    df_ = explode_data_from_column(df_, 'unit,sex,age,geo', split=',')
    df_ = rename_cols(df_)
    df_ = extract_value_number(df_)
    df_ = change_dtypes(df_)
    df_ = drop_missing_value(df_)
    df_ = filter_region(df_, r)
    df_ = select_columns(df_)

    return df_


if __name__ == "__main__":  # pragma: no cover

    args = parse_args()
    df = load_data()
    df = clean_data(df, r=args.region)
    save_data(df)
