import pandas as pd

# ========= 基本設定 =========
TOTAL_CAPITAL = 1000000      # 總資金（自己改）
DAILY_USE_RATIO = 0.4        # 每日最多動用
MAX_PER_STOCK = 0.1          # 單檔最大10%

INPUT_FILE = "trading_system_plan.csv"
OUTPUT_FILE = "trade_plan.csv"

# ========= 讀資料 =========
df = pd.read_csv(INPUT_FILE)

# ========= 分類邏輯 =========
def classify(row):
    flag = str(row.get("execution_flag", "")).upper()
    entry = str(row.get("entry_type", "")).upper()

    if row["action"] == "SELL":
        return "SELL"

    if flag == "TOP" and entry == "BREAK":
        return "TOP_BREAK"

    if flag == "TOP":
        return "TOP"

    if entry == "BREAK":
        return "BREAK"

    if row["action"] == "TEST":
        return "TEST"

    return "NORMAL"

df["group"] = df.apply(classify, axis=1)

# ========= 權重設定 =========
WEIGHTS = {
    "TOP_BREAK": 0.20,
    "TOP": 0.15,
    "BREAK": 0.10,
    "NORMAL": 0.05,
    "TEST": 0.03,
    "SELL": 0
}

# ========= 計算資金 =========
usable_capital = TOTAL_CAPITAL * DAILY_USE_RATIO

df["weight"] = df["group"].map(WEIGHTS)

# 各組數量
group_counts = df["group"].value_counts().to_dict()

def calc_amount(row):
    g = row["group"]

    if g == "SELL":
        return 0

    weight = WEIGHTS.get(g, 0)

    count = group_counts.get(g, 1)

    amount = usable_capital * weight / count

    # 單檔上限
    max_cap = TOTAL_CAPITAL * MAX_PER_STOCK

    return min(amount, max_cap)

df["suggested_amount"] = df.apply(calc_amount, axis=1)

# ========= 權重轉換 =========
df["target_weight"] = df["suggested_amount"] / TOTAL_CAPITAL

# ========= 輸出 =========
output_cols = [
    "stock_id",
    "action",
    "group",
    "score",
    "entry_type",
    "suggested_amount",
    "target_weight"
]

df[output_cols].to_csv(OUTPUT_FILE, index=False)

print("✅ trade_plan.csv 已產生")
