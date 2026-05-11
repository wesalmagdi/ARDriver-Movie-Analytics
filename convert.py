import pandas as pd

df = pd.read_json('mds.json')
try:
    df.to_csv('output.csv', index=False, encoding='utf-8')
    print("Success! File created.")
except Exception as e:
    print(f"An error occurred: {e}")
