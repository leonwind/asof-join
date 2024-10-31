import pandas as pd

df = pd.read_csv("../data/btc_usd_data_header.csv")
df["time"] = pd.to_datetime(df["time"], unit='s')
df.set_index("time", inplace=True)
df_resampled = df.resample("10S").interpolate(method="linear")
df_resampled.reset_index(inplace=True)
df_resampled.to_csv('resampled_data.csv', index=False)
