from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import vnstock, Quote, Listing, Company, Finance, Trading, Screener, config
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import numpy as np
import time
import random

# In version để debug
print("vnstock version:", vnstock.__version__)

# Enable proxy tự động cho v3.3.0
config.proxy_enabled = True

app = FastAPI()

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "Stock API is running (Shark Analysis + Foreign Flow Integrated)"}

# --- 1. HÀM HELPER: LẤY DỮ LIỆU AN TOÀN ---
def get_stock_data_safe(symbol: str, start_date: str, end_date: str, prefer_foreign: bool = True):
    print(f"Fetching data for {symbol} ({start_date} → {end_date}) - prefer_foreign={prefer_foreign}")
    
    sources = ['TCBS', 'MSN', 'VCI'] if prefer_foreign else ['VCI', 'TCBS', 'MSN']
    
    df = None
    for src in sources:
        print(f"→ Trying source: {src}")
        max_attempts = 4 if src == 'TCBS' else 2
        for attempt in range(1, max_attempts + 1):
            print(f"  Attempt {attempt}/{max_attempts}")
            try:
                quote = Quote(symbol=symbol, source=src, random_agent=True, proxy=True)
                print(f"  Quote init OK for {src}")
                
                df = quote.history(start=start_date, end=end_date, interval='1D')
                
                if df is not None and not df.empty:
                    print(f"  SUCCESS - {src} (attempt {attempt}) - Rows: {len(df)}")
                    print(f"  Columns: {list(df.columns)}")
                    break
                else:
                    print(f"  No data (df None or empty)")
            except Exception as e:
                err_str = str(e)
                print(f"  FAILED: {err_str}")
                if attempt < max_attempts and any(kw in err_str for kw in ['Connection', 'Timeout', 'RetryError']):
                    sleep_time = random.uniform(5, 15)
                    print(f"  Retry after {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                continue
        if df is not None and not df.empty:
            break
    
    if df is None or df.empty:
        print("→ All Quote sources failed → Trying  VCI fallback")
        try:
            stock = vnstock().stock(symbol=symbol, source='VCI')
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')
            if df is not None and not df.empty:
                print(f"  VCI fallback SUCCESS - Rows: {len(df)}")
                print(f"  Columns: {list(df.columns)}")
            else:
                print("  VCI fallback returned no data")
        except Exception as e:
            print(f"  VCI fallback FAILED: {str(e)}")

    # Fallback Trading.foreign_trading() cho foreign flow
    try:
        trading = Trading(source='TCBS')  # TCBS tốt nhất cho foreign
        df_foreign = trading.foreign_trading(symbol=symbol, start=start_date, end=end_date)
        if df_foreign is not None and not df_foreign.empty:
            print("Trading.foreign_trading SUCCESS - Rows:", len(df_foreign))
            df_foreign['date'] = pd.to_datetime(df_foreign.get('date', df_foreign.get('time'))).dt.strftime('%Y-%m-%d')
            df = df.merge(df_foreign[['date', 'buy_volume', 'sell_volume', 'net_volume']], on='date', how='left')
            df = df.rename(columns={'buy_volume': 'foreign_buy', 'sell_volume': 'foreign_sell', 'net_volume': 'foreign_net'})
            df['foreign_buy'] = df['foreign_buy'].fillna(0)
            df['foreign_sell'] = df['foreign_sell'].fillna(0)
            df['foreign_net'] = df['foreign_net'].fillna(0)
    except Exception as e:
        print(f"Trading.foreign_trading fallback failed: {e}")

    return df

# --- 2. API STOCK (OHLCV + Foreign + Shark) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = get_stock_data_safe(symbol, start_date, end_date, prefer_foreign=True)

        if df is None or df.empty:
            return {"error": "Không lấy được dữ liệu"}

        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        date_col = next((c for c in ['time', 'tradingdate', 'date', 'ngay'] if c in df.columns), None)
        if date_col:
            df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        elif hasattr(df.index, 'name') and df.index.name == 'time':
            df['date'] = df.index.strftime('%Y-%m-%d')
        else:
            df['date'] = ''

        if 'close' in df.columns and df['close'].iloc[-1] < 500:
            for c in ['open', 'high', 'low', 'close']:
                if c in df.columns:
                    df[c] *= 1000

        # Map foreign
        foreign_buy_candidates = ['foreign_buy', 'nn_mua', 'buy_foreign_volume', 'buy_foreign_qtty', 'nn_buy_vol', 'foreign_buy_vol']
        foreign_sell_candidates = ['foreign_sell', 'nn_ban', 'sell_foreign_volume', 'sell_foreign_qtty', 'nn_sell_vol', 'foreign_sell_vol']
        foreign_net_candidates = ['net_foreign_volume', 'nn_net_vol', 'khoi_ngoai_rong', 'net_foreign', 'foreign_net_vol', 'net_value']

        df['foreign_buy'] = 0.0
        df['foreign_sell'] = 0.0
        df['foreign_net'] = 0.0

        for idx, row in df.iterrows():
            for col in foreign_buy_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_buy'] = float(row[col])
                    break
            for col in foreign_sell_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_sell'] = float(row[col])
                    break
            df.at[idx, 'foreign_net'] = df.at[idx, 'foreign_buy'] - df.at[idx, 'foreign_sell']
            if df.at[idx, 'foreign_net'] == 0:
                for col in foreign_net_candidates:
                    if col in df.columns and pd.notna(row[col]):
                        df.at[idx, 'foreign_net'] = float(row[col])
                        break

        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        vol_current = last['volume']
        vol_avg = last['ma20_vol'] if not pd.isna(last['ma20_vol']) else 1
        vol_ratio = vol_current / vol_avg

        price_change_pct = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] != 0 else 0

        foreign_net_today = last['foreign_net']
        cum_net_5d = last['cum_net_5d']
        foreign_ratio_today = last['foreign_ratio']

        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = "Không có tín hiệu rõ ràng"

        if foreign_net_today != 0:
            # Logic có foreign
            if vol_ratio > 1.5:
                if price_change_pct > 1.5 and foreign_net_today > 0:
                    shark_action = "Cá mập ngoại GOM mạnh"
                    shark_color = "strong_buy"
                    shark_detail = f"Vol nổ {vol_ratio:.1f}x + Net ngoại +{foreign_net_today:,.0f}"
                elif price_change_pct < -1.5 and foreign_net_today < 0:
                    shark_action = "Cá mập ngoại XẢ mạnh"
                    shark_color = "strong_sell"
                    shark_detail = f"Vol nổ {vol_ratio:.1f}x + Net ngoại {foreign_net_today:,.0f}"
                elif foreign_net_today > 100000:
                    shark_action = "Ngoại mua chủ động"
                    shark_color = "buy"
                else:
                    shark_action = "Biến động mạnh (có thể cá mập)"
                    shark_color = "warning"
        else:
            # Thuần volume/price
            if vol_ratio > 1.3:
                if price_change_pct > 1.5:
                    shark_action = "Gom hàng mạnh"
                    shark_color = "strong_buy"
                    shark_detail = f"Vol nổ {vol_ratio:.1f}x + Giá tăng {price_change_pct:.1f}%"
                elif price_change_pct < -1.5:
                    shark_action = "Xả hàng mạnh"
                    shark_color = "strong_sell"
                    shark_detail = f"Vol nổ {vol_ratio:.1f}x + Giá giảm {price_change_pct:.1f}%"
                else:
                    shark_action = "Biến động mạnh (có thể cá mập)"
                    shark_color = "warning"
                    shark_detail = f"Vol cao {vol_ratio:.1f}x nhưng giá sideway"

            elif vol_ratio < 0.6 and abs(price_change_pct) > 2:
                if price_change_pct > 2:
                    shark_action = "Kéo giá (tiết cung)"
                    shark_color = "buy"
                    shark_detail = f"Vol thấp {vol_ratio:.1f}x + Giá tăng mạnh {price_change_pct:.1f}%"
                elif price_change_pct < -2:
                    shark_action = "Đè giá (cạn vol)"
                    shark_color = "sell"
                    shark_detail = f"Vol thấp {vol_ratio:.1f}x + Giá giảm mạnh {price_change_pct:.1f}%"

        warning = None
        if foreign_net_today == 0 and cum_net_5d == 0:
            warning = "Dữ liệu khối ngoại không khả dụng (nguồn hiện tại chỉ VCI). Shark chỉ dựa trên volume/price."

        data_cols = ['date', 'open', 'high', 'low', 'close', 'volume',
                     'foreign_buy', 'foreign_sell', 'foreign_net', 'foreign_ratio']

        response = {
            "data": df[data_cols].fillna(0).to_dict(orient='records'),
            "latest": {
                "date": last.get('date', ''),
                "close": float(last['close']),
                "volume": float(vol_current),
                "foreign_net": float(foreign_net_today),
                "cum_net_5d": float(cum_net_5d)
            },
            "shark_analysis": {
                "action": shark_action,
                "color": shark_color,
                "detail": shark_detail,
                "vol_ratio": round(vol_ratio, 2),
                "price_change_pct": round(price_change_pct, 2),
                "foreign_ratio": round(foreign_ratio_today, 3),
                "foreign_net_today": float(foreign_net_today)
            }
        }

        if warning:
            response["warning"] = warning

        return response

    except Exception as e:
        print(f"Stock Error {symbol}: {e}")
        return {"error": str(e)}

# --- 3. API KHỐI NGOẠI RIÊNG ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    try:
        symbol = symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        df = get_stock_data_safe(symbol, start_date, end_date, prefer_foreign=True)

        if df is None or df.empty:
            return []

        df.columns = [col.lower().replace(' ', '_') for col in df.columns]

        results = []
        for _, row in df.iterrows():
            buy = 0.0
            sell = 0.0

            for col in ['foreign_buy', 'nn_mua', 'buy_foreign_volume', 'nn_buy_vol']:
                if col in row and pd.notna(row[col]):
                    buy = float(row[col])
                    break

            for col in ['foreign_sell', 'nn_ban', 'sell_foreign_volume', 'nn_sell_vol']:
                if col in row and pd.notna(row[col]):
                    sell = float(row[col])
                    break

            net = buy - sell

            if net == 0:
                for col in ['net_foreign_volume', 'nn_net_vol', 'khoi_ngoai_rong', 'net_foreign']:
                    if col in row and pd.notna(row[col]):
                        net = float(row[col])
                        break

            results.append({
                "date": str(row.get('date') or row.get('time') or row.get('tradingdate') or ''),
                "buyVol": buy,
                "sellVol": sell,
                "netVolume": net
            })

        return results

    except Exception as e:
        print(f"Foreign Error {symbol}: {e}")
        return []

# --- 4. API TIN TỨC ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn)'
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=vi&gl=VN&ceid=VN:vi"
        feed = feedparser.parse(rss_url)
        
        news_list = []
        for entry in feed.entries[:10]:
            published_parsed = entry.get("published_parsed")
            date_str = f"{published_parsed.tm_year}-{published_parsed.tm_mon:02d}-{published_parsed.tm_mday:02d}" if published_parsed else ""
            
            news_list.append({
                "title": entry.title,
                "link": entry.link,
                "publishdate": date_str,
                "source": "Google News"
            })
        return news_list
    except Exception as e:
        print(f"News Error {symbol}: {e}")
        return []

# --- 5. API CHỈ SỐ THỊ TRƯỜNG (fallback dùng Quote) ---
@app.get("/api/index/{index_symbol}")
def get_index_data(index_symbol: str):
    try:
        index_symbol = index_symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = get_stock_data_safe(index_symbol, start_date, end_date, prefer_foreign=False)
        if df is not None and not df.empty:
            print(f"Index data for {index_symbol} - Rows: {len(df)}")
            return df.to_dict(orient='records')
        else:
            return {"error": "Không lấy được dữ liệu chỉ số"}
    except Exception as e:
        print(f"Index Error {index_symbol}: {e}")
        return {"error": str(e)}

# --- 6. API TOP MOVER (nếu market_top_mover tồn tại) ---
@app.get("/api/top_mover")
def get_top_mover(filter: str = 'ForeignTrading', limit: int = 10):
    try:
        df = market_top_mover(filter=filter, limit=limit)
        if df is not None and not df.empty:
            print(f"Top mover for {filter} - Rows: {len(df)}")
            return df.to_dict(orient='records')
        else:
            return {"error": "Không lấy được dữ liệu top mover"}
    except Exception as e:
        print(f"Top Mover Error {filter}: {e}")
        return {"error": "Chức năng top mover chưa hỗ trợ"}

# --- 7. API REALTIME (price_board nếu tồn tại) ---
@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        symbol = symbol.upper()
        df = price_board(symbol)
        if df is not None and not df.empty:
            print(f"Realtime data for {symbol}")
            return df.to_dict(orient='records')
        else:
            return {"error": "Không lấy được dữ liệu realtime"}
    except Exception as e:
        print(f"Realtime Error {symbol}: {e}")
        return {"error": "Chức năng realtime chưa hỗ trợ"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

