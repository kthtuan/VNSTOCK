from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import vnstock as vnstock_lib
from vnstock import Quote, Trading, config
try:
    from vnstock import market_top_mover
except ImportError:
    market_top_mover = None

import pandas as pd
from datetime import datetime, timedelta
import urllib.parse
import numpy as np
import time
import requests # Cần import requests để giả lập trình duyệt

# --- CONFIG ---
print("vnstock loaded from:", vnstock_lib.__file__)
if hasattr(config, 'proxy_enabled'):
    config.proxy_enabled = True

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STOCK_CACHE = {}
CACHE_DURATION = 300 # Cache 5 phút

@app.get("/")
def home():
    return {"message": "Stock API Ultimate (Direct SSI + Shark V2)"}

# --- 1. HÀM ĐẶC NHIỆM: GỌI TRỰC TIẾP SSI (Fake Browser) ---
def get_foreign_direct_ssi(symbol: str, start_date: str, end_date: str):
    """
    Hàm này tự gọi API của SSI, giả danh trình duyệt Chrome để tránh bị chặn IP trên Render.
    """
    print(f"🕵️  Direct Fetch SSI for {symbol}...")
    
    url = "https://iboard.ssi.com.vn/dchart/api/history"
    
    # Headers giả lập Chrome Windows
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://iboard.ssi.com.vn/",
        "Origin": "https://iboard.ssi.com.vn",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }
    
    try:
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    except:
        end_ts = int(time.time())
        start_ts = end_ts - 31536000 # 1 năm

    params = {
        "resolution": "D",
        "symbol": symbol,
        "from": start_ts,
        "to": end_ts
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if "t" in data and len(data["t"]) > 0:
                df = pd.DataFrame({
                    "date": pd.to_datetime(data["t"], unit='s').strftime('%Y-%m-%d'),
                    "close": data["c"],
                    "open": data["o"],
                    "high": data["h"],
                    "low": data["l"],
                    "volume": data["v"]
                })
                
                # SSI Direct API /history cơ bản thường không trả kèm Foreign
                # Tuy nhiên ta cứ tạo cột placeholder để code phía sau không lỗi
                # Nếu muốn chuẩn Foreign từ Direct, cần gọi thêm API /foreignTrading riêng
                df['foreign_buy'] = 0.0
                df['foreign_sell'] = 0.0
                
                return df
                
        print(f"  -> Direct SSI Failed: Status {response.status_code}")
    except Exception as e:
        print(f"  -> Direct SSI Error: {e}")
        
    return None

# --- 2. CORE PROCESSING ---
def process_dataframe(df):
    if df is None or df.empty: return None
    df.columns = [col.lower() for col in df.columns]
    
    date_col = next((c for c in ['date', 'time', 'trading_date'] if c in df.columns), None)
    if date_col and date_col != 'date':
        try:
            df['date'] = pd.to_datetime(df[date_col], dayfirst=True).dt.strftime('%Y-%m-%d')
        except:
            df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
            
    df = df.sort_values('date')
    
    # Ensure columns exist
    for col in ['close', 'volume', 'foreign_buy', 'foreign_sell']:
        if col not in df.columns: df[col] = 0.0
    
    # Mapping Foreign columns if available (from Vnstock Quote SSI)
    if 'buy_foreign_quantity' in df.columns: df['foreign_buy'] = df['buy_foreign_quantity']
    if 'sell_foreign_quantity' in df.columns: df['foreign_sell'] = df['sell_foreign_quantity']
    if 'foreign_buy_volume' in df.columns: df['foreign_buy'] = df['foreign_buy_volume']
    if 'foreign_sell_volume' in df.columns: df['foreign_sell'] = df['foreign_sell_volume']

    df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']
    
    # Fix đơn vị giá
    if not df.empty and df['close'].iloc[-1] < 500:
        for c in ['open', 'high', 'low', 'close']:
            if c in df.columns: df[c] = df[c] * 1000
            
    return df

def get_data_robust(symbol: str, start_date: str, end_date: str):
    # CÁCH 1: SSI DIRECT REQUEST (Mạnh nhất trên Render)
    df_direct = get_foreign_direct_ssi(symbol, start_date, end_date)
    if df_direct is not None and not df_direct.empty:
        return process_dataframe(df_direct), "Dữ liệu từ SSI Direct (Có thể thiếu Foreign Flow)."

    # CÁCH 2: VNSTOCK QUOTE (SSI)
    try:
        print("  -> Fallback to Vnstock Quote(SSI)...")
        quote = Quote(symbol=symbol, source='SSI')
        df = quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty:
            return process_dataframe(df), None
    except:
        pass

    # CÁCH 3: VNSTOCK QUOTE (VCI)
    try:
        print("  -> Fallback to Vnstock Quote(VCI)...")
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None:
            return process_dataframe(df), "Dữ liệu dự phòng từ VCI Quote (Không có khối ngoại)."
    except:
        pass

    return None, "Không lấy được dữ liệu."

# --- API ENDPOINTS ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        if symbol in STOCK_CACHE:
            if current_time - STOCK_CACHE[symbol]['timestamp'] < CACHE_DURATION:
                return STOCK_CACHE[symbol]['data']

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        df, warning = get_data_robust(symbol, start_date, end_date)
        
        if df is None: return {"error": warning}

        # Tính toán chỉ số
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        # --- SHARK ANALYSIS V2 ---
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = f"Vol {vol_ratio:.1f}x, Giá {price_change:.1f}%"
        
        IS_VOL_SPIKE = vol_ratio > 1.3
        IS_PRICE_UP = price_change > 2.0
        IS_PRICE_DOWN = price_change < -2.0
        IS_FOREIGN_BUY = last['foreign_net'] > 0
        IS_FOREIGN_SELL = last['foreign_net'] < 0
        
        if IS_VOL_SPIKE:
            if IS_PRICE_UP:
                if IS_FOREIGN_BUY: 
                    shark_action, shark_color = "Gom hàng mạnh (Uy tín)", "strong_buy"
                elif IS_FOREIGN_SELL: 
                    shark_action, shark_color = "Coi chừng Kéo Xả (FOMO)", "warning"
                else: 
                    shark_action, shark_color = "Dòng tiền đầu cơ nóng", "buy"
            elif IS_PRICE_DOWN:
                if IS_FOREIGN_BUY: 
                    shark_action, shark_color = "Đè gom (Hoảng loạn)", "buy"
                else: 
                    shark_action, shark_color = "Xả hàng mạnh", "strong_sell"
            else:
                shark_action = "Biến động mạnh"

        result = {
            "data": df[['date', 'open', 'high', 'low', 'close', 'volume', 'foreign_buy', 'foreign_sell', 'foreign_net', 'foreign_ratio']].fillna(0).to_dict(orient='records'),
            "latest": {
                "date": last['date'],
                "close": float(last['close']),
                "volume": float(last['volume']),
                "foreign_net": float(last['foreign_net'])
            },
            "shark_analysis": {
                "action": shark_action,
                "color": shark_color,
                "detail": shark_detail,
                "vol_ratio": round(vol_ratio, 2),
                "price_change_pct": round(price_change, 2),
                "foreign_net_today": float(last['foreign_net'])
            }
        }
        if warning: result["warning"] = warning
        
        STOCK_CACHE[symbol] = {'timestamp': current_time, 'data': result}
        return result

    except Exception as e:
        return {"error": str(e)}

@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn)'
        rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=vi&gl=VN&ceid=VN:vi"
        feed = feedparser.parse(rss_url)
        return [{"title": e.title, "link": e.link, "publishdate": f"{e.published_parsed.tm_year}-{e.published_parsed.tm_mon:02d}-{e.published_parsed.tm_mday:02d}" if e.get("published_parsed") else "", "source": "Google"} for e in feed.entries[:10]]
    except: return []

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        trading = Trading(source='VCI')
        df = trading.price_board([symbol.upper()])
        return df.to_dict(orient='records') if df is not None else {"error": "No Data"}
    except Exception as e: return {"error": str(e)}

@app.get("/api/top_mover")
def get_top_mover(filter: str = 'ForeignTrading', limit: int = 10):
    try:
        if market_top_mover:
            df = market_top_mover(filter=filter, limit=limit)
            if df is not None: return df.to_dict(orient='records')
        return {"error": "Not Supported"}
    except: return {"error": "Error"}

@app.get("/api/index/{index_symbol}")
def get_index_data(index_symbol: str):
    try:
        index_symbol = index_symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        df, _ = get_data_robust(index_symbol, start_date, end_date)
        if df is not None:
             return df.to_dict(orient='records')
        return {"error": "Không lấy được dữ liệu chỉ số"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
