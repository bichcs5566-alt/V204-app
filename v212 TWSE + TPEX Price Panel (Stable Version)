# v212 TWSE + TPEX Price Panel (Stable Version)

import pandas as pd
import numpy as np
import os

def build_price_panel(input_path="v202_positions.csv", output_path="v212_price_panel.csv"):
    df = pd.read_csv(input_path)

    # Ensure required columns
    required_cols = ["date", "symbol", "close"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Convert date
    df["date"] = pd.to_datetime(df["date"])

    # Pivot to price panel
    panel = df.pivot_table(index="date", columns="symbol", values="close")

    # Sort index
    panel = panel.sort_index()

    # Forward fill missing prices
    panel = panel.ffill()

    # Save
    panel.to_csv(output_path)

    print("v212 build complete")
    print(f"Rows: {len(panel)} | Columns: {len(panel.columns)}")

if __name__ == "__main__":
    build_price_panel()
