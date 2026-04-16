import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_macro_score():
    today = datetime.today()
    dates = [today - timedelta(days=i) for i in range(200)]
    dates = sorted(dates)

    data = []

    for i, d in enumerate(dates):

        # 🔥 模擬「市場週期」
        cycle = (i // 40) % 3
        # 每40天切一次 regime

        if cycle == 0:
            # 多頭
            base = 0.8
            noise = np.random.normal(0, 0.1)
        elif cycle == 1:
            # 中性
            base = 0.0
            noise = np.random.normal(0, 0.15)
        else:
            # 空頭
            base = -0.8
            noise = np.random.normal(0, 0.1)

        macro_score = base + noise

        # 限制範圍
        macro_score = max(min(macro_score, 1), -1)

        data.append([d.strftime("%Y-%m-%d"), macro_score])

    df = pd.DataFrame(data, columns=["trade_date", "macro_score"])
    df.to_csv("macro_signal.csv", index=False)

    print("v231.2 macro_signal.csv updated")


if __name__ == "__main__":
    generate_macro_score()
