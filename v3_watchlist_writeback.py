import argparse
from pathlib import Path
import pandas as pd

CSV_PATH = Path("watchlist.csv")


def load_watchlist() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame(columns=["stock_id"])

    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(CSV_PATH)

    df.columns = [str(c).strip().lower() for c in df.columns]

    rename_map = {}
    if "stock" in df.columns and "stock_id" not in df.columns:
        rename_map["stock"] = "stock_id"
    if "symbol" in df.columns and "stock_id" not in df.columns:
        rename_map["symbol"] = "stock_id"
    if "code" in df.columns and "stock_id" not in df.columns:
        rename_map["code"] = "stock_id"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "stock_id" not in df.columns:
        df["stock_id"] = None

    df = df[["stock_id"]].copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df = df[df["stock_id"] != ""]
    df = df.drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
    return df


def validate_stock_id(stock_id: str) -> str:
    stock_id = str(stock_id).strip()
    if not stock_id:
        raise ValueError("stock_id ä¸å¯ç©ºç½")
    return stock_id


def add_watch(df: pd.DataFrame, stock_id: str) -> pd.DataFrame:
    stock_id = validate_stock_id(stock_id)
    if (df["stock_id"] == stock_id).any():
        print(f"èªé¸è¡å·²å­å¨: {stock_id}")
        return df

    df = pd.concat([df, pd.DataFrame([{"stock_id": stock_id}])], ignore_index=True)
    print(f"å·²æ°å¢èªé¸è¡: {stock_id}")
    return df


def remove_watch(df: pd.DataFrame, stock_id: str) -> pd.DataFrame:
    stock_id = validate_stock_id(stock_id)
    before = len(df)
    df = df[df["stock_id"] != stock_id].copy()
    after = len(df)
    if before == after:
        print(f"æ¾ä¸å°èªé¸è¡: {stock_id}")
    else:
        print(f"å·²ç§»é¤èªé¸è¡: {stock_id}")
    return df


def save_watchlist(df: pd.DataFrame) -> None:
    df = df.copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df = df[df["stock_id"] != ""]
    df = df.drop_duplicates(subset=["stock_id"]).sort_values(["stock_id"]).reset_index(drop=True)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"å·²å¯«å {CSV_PATH}")


def main() -> None:
    parser = argparse.ArgumentParser(description="v3 èªé¸è¡å¯«åè³æ¬")
    parser.add_argument("--action", required=True, choices=["add", "remove"], help="add æ remove")
    parser.add_argument("--stock_id", required=True, help="è¡ç¥¨ä»£è")
    args = parser.parse_args()

    df = load_watchlist()

    if args.action == "add":
        df = add_watch(df, args.stock_id)
    elif args.action == "remove":
        df = remove_watch(df, args.stock_id)

    save_watchlist(df)
    print("v3_watchlist_writeback å®æ")


if __name__ == "__main__":
    main()
