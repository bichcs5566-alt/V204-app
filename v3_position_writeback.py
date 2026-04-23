import argparse
from pathlib import Path
import pandas as pd

CSV_PATH = Path("current_positions.csv")


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c).strip().lower() for c in df.columns]
    df.columns = cols

    rename_map = {}
    if "stock" in df.columns and "stock_id" not in df.columns:
        rename_map["stock"] = "stock_id"
    if "symbol" in df.columns and "stock_id" not in df.columns:
        rename_map["symbol"] = "stock_id"
    if "code" in df.columns and "stock_id" not in df.columns:
        rename_map["code"] = "stock_id"
    if "shares_qty" in df.columns and "shares" not in df.columns:
        rename_map["shares_qty"] = "shares"
    if "qty" in df.columns and "shares" not in df.columns:
        rename_map["qty"] = "shares"
    if "cost" in df.columns and "avg_cost" not in df.columns:
        rename_map["cost"] = "avg_cost"
    if "avgprice" in df.columns and "avg_cost" not in df.columns:
        rename_map["avgprice"] = "avg_cost"

    if rename_map:
        df = df.rename(columns=rename_map)

    for col in ["stock_id", "shares", "avg_cost"]:
        if col not in df.columns:
            df[col] = None

    return df[["stock_id", "shares", "avg_cost"]].copy()


def load_positions() -> pd.DataFrame:
    if not CSV_PATH.exists():
        return pd.DataFrame(columns=["stock_id", "shares", "avg_cost"])

    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(CSV_PATH)

    df = ensure_columns(df)
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce")
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce")
    df = df[df["stock_id"].notna() & (df["stock_id"] != "")]
    return df


def validate_stock_id(stock_id: str) -> str:
    stock_id = str(stock_id).strip()
    if not stock_id:
        raise ValueError("stock_id ä¸å¯ç©ºç½")
    return stock_id


def validate_shares(shares: str) -> int:
    try:
        value = int(float(str(shares).strip()))
    except Exception as exc:
        raise ValueError("shares å¿é çºæ­£æ´æ¸") from exc
    if value <= 0:
        raise ValueError("shares å¿é  > 0")
    return value


def validate_avg_cost(avg_cost: str) -> float:
    try:
        value = float(str(avg_cost).strip())
    except Exception as exc:
        raise ValueError("avg_cost å¿é çºæ¸å­") from exc
    if value <= 0:
        raise ValueError("avg_cost å¿é  > 0")
    return value


def add_position(df: pd.DataFrame, stock_id: str, shares: int, avg_cost: float) -> pd.DataFrame:
    stock_id = validate_stock_id(stock_id)
    shares = validate_shares(shares)
    avg_cost = validate_avg_cost(avg_cost)

    hit = df["stock_id"] == stock_id
    if hit.any():
        df.loc[hit, "shares"] = shares
        df.loc[hit, "avg_cost"] = avg_cost
        print(f"å·²è¦èæå: {stock_id}")
    else:
        df = pd.concat(
            [df, pd.DataFrame([{"stock_id": stock_id, "shares": shares, "avg_cost": avg_cost}])],
            ignore_index=True
        )
        print(f"å·²æ°å¢æå: {stock_id}")
    return df


def remove_position(df: pd.DataFrame, stock_id: str) -> pd.DataFrame:
    stock_id = validate_stock_id(stock_id)
    before = len(df)
    df = df[df["stock_id"] != stock_id].copy()
    after = len(df)
    if before == after:
        print(f"æ¾ä¸å°æå: {stock_id}")
    else:
        print(f"å·²ç§»é¤æå: {stock_id}")
    return df


def save_positions(df: pd.DataFrame) -> None:
    df = df.copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0).astype(int)
    df["avg_cost"] = pd.to_numeric(df["avg_cost"], errors="coerce")
    df = df[df["stock_id"] != ""]
    df = df.sort_values(["stock_id"]).reset_index(drop=True)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"å·²å¯«å {CSV_PATH}")


def main() -> None:
    parser = argparse.ArgumentParser(description="v3 æåå¯«åè³æ¬")
    parser.add_argument("--action", required=True, choices=["add", "remove"], help="add æ remove")
    parser.add_argument("--stock_id", required=True, help="è¡ç¥¨ä»£è")
    parser.add_argument("--shares", help="è¡æ¸ï¼add æå¿å¡«")
    parser.add_argument("--avg_cost", help="ææ¬ï¼add æå¿å¡«")
    args = parser.parse_args()

    df = load_positions()

    if args.action == "add":
        if args.shares is None or args.avg_cost is None:
            raise ValueError("add æå¿é æä¾ shares è avg_cost")
        df = add_position(df, args.stock_id, args.shares, args.avg_cost)
    elif args.action == "remove":
        df = remove_position(df, args.stock_id)

    save_positions(df)
    print("v3_position_writeback å®æ")


if __name__ == "__main__":
    main()
