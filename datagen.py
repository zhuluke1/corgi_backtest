import yfinance as yf
import pandas as pd
import time
from datetime import datetime, timedelta

# Load tickers from Excel file
print("Loading tickers from energy_universe.csv.xlsx...")
df = pd.read_excel("energy_universe.csv.xlsx")
tickers = df.iloc[:, 0].astype(str).str.strip().str.upper().unique().tolist()
tickers = [t for t in tickers if t and t != 'NAN' and pd.notna(t)]
print(f"Loaded {len(tickers)} tickers\n")

# Calculate date range (6 years)
end_date = datetime.now()
start_date = end_date - timedelta(days=6*365)
start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

# Get price data and shares data for each ticker, then calculate daily market cap
print("Downloading data and calculating daily market cap for each ticker...")
market_cap_data = {}
price_data_dict = {}
shares_data_dict = {}

for i, ticker in enumerate(tickers, 1):
    try:
        print(f"[{i}/{len(tickers)}] {ticker}...", end=' ')
        
        # Get ticker object
        ticker_obj = yf.Ticker(ticker)
        
        # Get daily price data using history()
        prices = ticker_obj.history(start=start_str, end=end_str, auto_adjust=True)
        
        # Get shares outstanding time series using get_shares_full()
        shares = ticker_obj.get_shares_full(start=None, end=None)
        
        if prices.empty:
            print("✗ (no price data)")
            continue
            
        if shares is None or len(shares) == 0:
            print("✗ (no shares data)")
            continue
        
        # Remove duplicate dates from prices (keep last value if duplicates exist)
        if prices.index.duplicated().any():
            prices = prices[~prices.index.duplicated(keep='last')]
        
        # Remove duplicate dates from shares data (keep last value if duplicates exist)
        if shares.index.duplicated().any():
            shares = shares[~shares.index.duplicated(keep='last')]
        
        # Remove timezone from indices if present
        if hasattr(prices.index, 'tz') and prices.index.tz is not None:
            prices.index = prices.index.tz_localize(None) if prices.index.tz is not None else prices.index
        if hasattr(shares.index, 'tz') and shares.index.tz is not None:
            shares.index = shares.index.tz_localize(None) if shares.index.tz is not None else shares.index
        
        # Store price and shares data
        price_data_dict[ticker] = prices
        shares_data_dict[ticker] = shares
        
        # Align shares data with price data dates
        # Forward fill shares data to match price dates
        shares_aligned = shares.reindex(prices.index).ffill().bfill()
        
        # Calculate market cap = Close price * Shares outstanding
        if 'Close' in prices.columns:
            market_cap = prices['Close'] * shares_aligned
            market_cap_data[ticker] = market_cap
            print(f"✓ ({len(market_cap)} days)")
        else:
            print("✗ (no Close price)")
            
        time.sleep(0.1)  # Be polite to API
        
    except Exception as e:
        print(f"✗ Error: {e}")
        continue

print(f"\nCalculated daily market cap for {len(market_cap_data)} tickers")

# Save to Excel
output_file = "yfinance_data.xlsx"
print(f"\nSaving to {output_file}...")

try:
    with pd.ExcelWriter(output_file, engine="openpyxl", mode='w') as writer:
        # Save price data
        for ticker, prices in price_data_dict.items():
            # Remove timezone from index if present
            prices_to_save = prices.copy()
            if hasattr(prices_to_save.index, 'tz') and prices_to_save.index.tz is not None:
                prices_to_save.index = prices_to_save.index.tz_localize(None)
            prices_to_save.to_excel(writer, sheet_name=f"{ticker}_prices")
        
        # Save shares data
        for ticker, shares in shares_data_dict.items():
            df = shares.reset_index()
            df.columns = ['Date', 'Shares_Outstanding']
            # Remove timezone from Date column if present (convert to naive datetime)
            df['Date'] = pd.to_datetime(df['Date'])
            if df['Date'].dt.tz is not None:
                df['Date'] = df['Date'].dt.tz_convert('UTC').dt.tz_localize(None)
            df.to_excel(writer, sheet_name=f"{ticker}_shares", index=False)
        
        # Save market cap data (daily time series)
        for ticker, market_cap in market_cap_data.items():
            df = market_cap.reset_index()
            df.columns = ['Date', 'Market_Cap']
            # Remove timezone from Date column if present (convert to naive datetime)
            df['Date'] = pd.to_datetime(df['Date'])
            if df['Date'].dt.tz is not None:
                df['Date'] = df['Date'].dt.tz_convert('UTC').dt.tz_localize(None)
            df.to_excel(writer, sheet_name=f"{ticker}_market_cap", index=False)
    
    print(f"✓ Done! Saved to {output_file}")
    print(f"   - Price data: {len(price_data_dict)} tickers")
    print(f"   - Shares data: {len(shares_data_dict)} tickers")
    print(f"   - Market cap data: {len(market_cap_data)} tickers")
    
except PermissionError:
    print(f"\n✗ ERROR: Cannot save to {output_file}")
    print("   The file is currently open in another program (likely Excel).")
    print("   Please close the file and run the script again.")
except Exception as e:
    print(f"\n✗ ERROR: Failed to save file: {e}")
