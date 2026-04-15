import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 🔧 模擬 macro 指標（之後可換 API）
def generate_macro_score():
    today = datetime.today()

    dates = [today - timedelta(days=i) for i in range(200)]
    dates = sorted(dates)

    data = []

    for d in dates:
        # 🔥 模擬三大結構（之後替換真數據）
        us_consumption = np.random.normal(0.2, 0.3)
        cn_pmi = np.random.normal(0.1, 0.3)
        tw_liquidity = np.random.normal(0.15, 0.25)

        # 👉 加權（這就是你原本架構）
        macro_score = (
            us_consumption * 0.4 +
            cn_pmi * 0.3 +
            tw_liquidity * 0.3
        )

        macro_score = max(min(macro_score, 1), -1)

        data.append([d.strftime("%Y-%m-%d"), macro_score])

    df = pd.DataFrame(data, columns=["trade_date", "macro_score"])
    df.to_csv("macro_signal.csv", index=False)

    print("macro_signal.csv updated")


if __name__ == "__main__":
    generate_macro_score()
