import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# Configuration
EXCEL_FILE = "yfinance_data.xlsx"
TOP_N = 50
INITIAL_VALUE = 10000
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Loading market cap data...")

# Load market cap data
sheet_names = pd.ExcelFile(EXCEL_FILE).sheet_names
market_cap_panel = None

# Preferred: single sheet named 'Market_Cap_Table' with first column = Date(s)
if 'Market_Cap_Table' in sheet_names:
    mcap_df = pd.read_excel(EXCEL_FILE, sheet_name='Market_Cap_Table')
    first_col = mcap_df.columns[0]
    mcap_df[first_col] = pd.to_datetime(mcap_df[first_col])
    mcap_df = mcap_df.rename(columns={first_col: 'Date'}).set_index('Date').sort_index()
    market_cap_panel = mcap_df
    print(f"Loaded Market_Cap_Table with {market_cap_panel.shape[0]} days × {market_cap_panel.shape[1]} tickers")
else:
    # Fallback: per-ticker sheets named '{TICKER}_market_cap'
    market_cap_dict = {}
    for sheet in sheet_names:
        if sheet.endswith("_market_cap"):
            ticker = sheet.replace("_market_cap", "")
            try:
                df = pd.read_excel(EXCEL_FILE, sheet_name=sheet)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date').sort_index()
                market_cap_dict[ticker] = df['Market_Cap']
            except:
                pass
    print(f"Loaded {len(market_cap_dict)} tickers from per-ticker market cap sheets")
    if market_cap_dict:
        all_dates = sorted(set().union(*[s.index for s in market_cap_dict.values()]))
        market_cap_panel = pd.DataFrame(index=all_dates, columns=list(market_cap_dict.keys()))
        for ticker, series in market_cap_dict.items():
            market_cap_panel[ticker] = series
        market_cap_panel = market_cap_panel.sort_index()
    else:
        raise RuntimeError("No market cap data found. Provide 'Market_Cap_Table' or per-ticker '_market_cap' sheets.")

# Load price data
print("Loading price data...")
price_dict = {}
for sheet in sheet_names:
    if sheet.endswith("_prices"):
        ticker = sheet.replace("_prices", "")
        try:
            df = pd.read_excel(EXCEL_FILE, sheet_name=sheet, index_col=0)
            df.index = pd.to_datetime(df.index)
            if 'Close' in df.columns:
                price_dict[ticker] = df['Close']
        except:
            pass

# Create price panel
price_panel = pd.DataFrame(index=market_cap_panel.index, columns=list(price_dict.keys()))
for ticker, series in price_dict.items():
    price_panel[ticker] = series
price_panel = price_panel.sort_index()

# Calculate returns
# If price sheets are available, use price returns; otherwise, approximate with market cap changes
if len(price_dict) > 0:
    returns_panel = price_panel.pct_change().fillna(0)
else:
    print("No price sheets found. Approximating returns from market cap changes.")
    returns_panel = market_cap_panel.pct_change().fillna(0)

# Quarterly rebalance dates
rebalance_dates = []
for year in market_cap_panel.index.year.unique():
    for quarter in [1, 2, 3, 4]:
        quarter_data = market_cap_panel[
            (market_cap_panel.index.year == year) & 
            (market_cap_panel.index.quarter == quarter)
        ]
        if not quarter_data.empty:
            rebalance_dates.append(quarter_data.index[-1])
rebalance_dates = sorted(set(rebalance_dates))

print(f"Rebalancing quarterly: {len(rebalance_dates)} dates")

# Backtest
print("Running backtest...")
portfolio_value = pd.Series(index=market_cap_panel.index, dtype=float)
portfolio_value.iloc[0] = INITIAL_VALUE
holdings = {}

for i, date in enumerate(market_cap_panel.index[1:], start=1):
    prev_date = market_cap_panel.index[i-1]
    
    # Rebalance on quarterly dates
    if date in rebalance_dates:
        mcap = market_cap_panel.loc[date].dropna()
        if len(mcap) >= TOP_N:
            top_tickers = mcap.nlargest(TOP_N).index.tolist()
            total_mcap = mcap[top_tickers].sum()
            holdings = {t: mcap[t] / total_mcap for t in top_tickers}
    
    # Calculate daily return
    if holdings:
        daily_return = sum(
            holdings.get(t, 0) * returns_panel.loc[date, t]
            for t in holdings.keys()
            if t in returns_panel.columns and pd.notna(returns_panel.loc[date, t])
        )
        portfolio_value.iloc[i] = portfolio_value.iloc[i-1] * (1 + daily_return)
    else:
        portfolio_value.iloc[i] = portfolio_value.iloc[i-1]

# Calculate returns
returns = portfolio_value.pct_change().dropna()
total_return = (portfolio_value.iloc[-1] / portfolio_value.iloc[0]) - 1
cagr = (1 + total_return) ** (252 / len(returns)) - 1

print(f"\nResults:")
print(f"  Total Return: {total_return:.2%}")
print(f"  CAGR: {cagr:.2%}")
print(f"  Final Value: ${portfolio_value.iloc[-1]:,.2f}")

# Create graph
plt.figure(figsize=(14, 8))
plt.plot(portfolio_value.index, portfolio_value.values, linewidth=2, color='#2E86AB')
plt.title(f'Top {TOP_N} Market Cap Weighted ETF - Portfolio Performance', fontsize=16, fontweight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Portfolio Value ($)', fontsize=12)
plt.grid(True, alpha=0.3)
plt.axhline(y=INITIAL_VALUE, color='r', linestyle='--', alpha=0.5, label=f'Starting: ${INITIAL_VALUE:,}')
plt.text(portfolio_value.index[-1], portfolio_value.iloc[-1], 
         f'${portfolio_value.iloc[-1]:,.0f}\n({total_return:.1%})', 
         fontsize=10, verticalalignment='bottom')
plt.legend()
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/portfolio_performance.png", dpi=300, bbox_inches='tight')
print(f"\nGraph saved to {OUTPUT_DIR}/portfolio_performance.png")
plt.show()

# Save portfolio value
portfolio_df = pd.DataFrame({
    'Date': portfolio_value.index,
    'Portfolio_Value': portfolio_value.values,
    'Daily_Return': portfolio_value.pct_change().values
})
portfolio_df.to_csv(f"{OUTPUT_DIR}/portfolio_value.csv", index=False)
print(f"Data saved to {OUTPUT_DIR}/portfolio_value.csv")

