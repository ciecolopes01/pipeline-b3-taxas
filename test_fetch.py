import pandas as pd
import requests
import urllib3
from datetime import date

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_curve(dt: date, curve_id: str):
    data_str = dt.strftime("%d/%m/%Y")
    data1_str = dt.strftime("%Y%m%d")
    url = f"https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-taxas-referenciais-bmf-ptBR.asp"
    params = {
        "Data": data_str,
        "Data1": data1_str,
        "slcTaxa": curve_id
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    print(f"Fetching {curve_id} for {data_str}...")
    response = requests.get(url, params=params, headers=headers, verify=False, timeout=30)
    response.raise_for_status()
    
    try:
        dfs = pd.read_html(response.text, decimal=',', thousands='.')
        if dfs:
            df = dfs[0]
            print(f"Success. Found {len(df)} rows.")
            print(df.head())
            return df
        else:
            print("No tables found in HTML.")
            return None
    except ValueError as e:
        print(f"Error parsing HTML: {e}")
        return None

if __name__ == "__main__":
    test_dt = date(2026, 3, 6)
    for curve in ["PRE", "APR", "TR"]:
        fetch_curve(test_dt, curve)
