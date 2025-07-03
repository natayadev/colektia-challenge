import pandas as pd
import polars as pl
from typing import Dict, Union


def group_by_customer_and_product(csv_path: str) -> pd.DataFrame:
    """
    Exercise 1: Complex grouping with pandas.
    Calculate total amount spent per customer per product,
    ordered by customer and descending amount.
    """
    df = pd.read_csv(csv_path, parse_dates=['fecha_registro'], dayfirst=True)
    result = (
    df.groupby(['cliente_id', 'producto'])['monto']
        .sum()
        .reset_index()
        .sort_values(['cliente_id', 'monto'], ascending=[True, False])
        .reset_index(drop=True)
    )
    return result


def filter_and_aggregate_active_records_polars(csv_path: str) -> pl.DataFrame:
    """
    Exercise 2: Transformation using Polars LazyFrame.
    Filter rows where 'estado' == 'activo',
    extract year from 'fecha_registro', 
    group by year and count records.
    """
    lazy_frame = (
        pl.read_csv(csv_path)
        .lazy()
        .filter(pl.col("estado") == "activo")
        .with_columns([
            pl.col("fecha_registro").str.strptime(pl.Date, "%d/%m/%Y").alias("parsed_date"),
        ])
        .with_columns([
            pl.col("parsed_date").dt.year().alias("registration_year")
        ])
        .group_by("registration_year")
        .agg(pl.count().alias("record_count"))
        .sort("registration_year")
        .collect()
    )
    return lazy_frame


def calculate_date_range_and_diff(df: pd.DataFrame, date_column: str) -> Dict[str, Union[pd.Timestamp, int]]:
    """
    Exercise 4: Given an unordered date column,
    find minimum date, maximum date and days difference.
    """
    dates = pd.to_datetime(df[date_column])
    min_date = dates.min()
    max_date = dates.max()
    days_difference = (max_date - min_date).days

    return f"Fecha mínima: {min_date}, Fecha máxima: {max_date}, Diferencia en días: {days_difference}"


if __name__ == "__main__":
    RAW_CSV_PATH = "data/raw/ventas_perfumeria.csv"

    # Exercise 1
    grouped_df = group_by_customer_and_product(RAW_CSV_PATH)
    print("Exercise 1: Total spent per customer and product")
    print(grouped_df.head(10))

    # Exercise 2
    polars_df = filter_and_aggregate_active_records_polars(RAW_CSV_PATH)
    print("\nExercise 2: Active records count per registration year")
    print(polars_df)

    # Exercise 4
    original_df = pd.read_csv(RAW_CSV_PATH, parse_dates=['fecha_registro'], dayfirst=True)
    date_range_info = calculate_date_range_and_diff(original_df, "fecha_registro")
    print("\nExercise 4: Date range and difference in days")
    print(date_range_info)
