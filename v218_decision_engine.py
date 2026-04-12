# v218_decision_engine.py
# v218（Alpha 判決引擎）

import pandas as pd
import numpy as np

INITIAL_CAPITAL = 100000

MODES = {
    "A_only": ["A"],
    "B_only": ["B"],
    "AB": ["A", "B"]
}


def load_data():
    df = pd.read_csv("v216_positions.csv")
    return df


def prepare(df):
    df = df.copy()

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")

    # 建立 wret
    if "wret" not in df.columns:
        df["wret"] = (
            pd.to_numeric(df["weight_portfolio"], errors="coerce").fillna(0)
            * pd.to_numeric(df["trade_ret"], errors="coerce").fillna(0)
        )

    df = df.dropna(subset=["trade_date"])
    df = df.sort_values("trade_date")

    return df


def run_mode(df, engines):
    d = df[df["engine"].isin(engines)].copy()

    if d.empty:
        return {
            "return": 0,
            "sharpe": 0,
            "mdd": 0
        }

    daily = d.groupby("trade_date")["wret"].sum().reset_index()

    daily["nav"] = INITIAL_CAPITAL * (1 + daily["wret"]).cumprod()

    total_return = daily["nav"].iloc[-1] / INITIAL_CAPITAL - 1

    sharpe = (
        daily["wret"].mean()
        / (daily["wret"].std() + 1e-9)
        * np.sqrt(252)
    )

    mdd = (daily["nav"] / daily["nav"].cummax() - 1).min()

    return {
        "return": float(total_return),
        "sharpe": float(sharpe),
        "mdd": float(mdd)
    }


def score(r):
    # 核心評分（偏向Sharpe）
    return r["sharpe"] * 2 + r["return"] - abs(r["mdd"])


def decide(winner, results):
    if winner == "B_only":
        return "DROP_A_USE_B"
    elif winner == "A_only":
        return "USE_A_ONLY"
    else:
        # 判斷是否值得混合
        if results["AB"]["score"] > results["B_only"]["score"]:
            return "KEEP_AB"
        else:
            return "PREFER_B"


def main():
    df = prepare(load_data())

    results = {}

    for name, engines in MODES.items():
        r = run_mode(df, engines)
        r["score"] = score(r)
        results[name] = r

    out = pd.DataFrame(results).T
    out.to_csv("v218_compare_decision.csv")

    winner = out["score"].idxmax()
    decision = decide(winner, results)

    # 輸出判決
    with open("v218_decision.txt", "w") as f:
        f.write("=== v218 Alpha Decision ===\n")
        f.write(f"WINNER: {winner}\n")
        f.write(f"DECISION: {decision}\n\n")

        f.write("=== Detail ===\n")
        f.write(out.to_string())

    print("==== RESULT ====")
    print(out)
    print("\nWINNER:", winner)
    print("DECISION:", decision)


if __name__ == "__main__":
    main()
