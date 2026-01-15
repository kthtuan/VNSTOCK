from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import vnstock as vnstock_lib
from vnstock import Quote, Trading, config
# Thử import market_top_mover
try:
    from vnstock import market_top_mover
except ImportError:
    market_top_mover = None

import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import numpy as np
import time

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

# --- GLOBAL CACHE ---
STOCK_CACHE = {}
CACHE_DURATION = 300 # 5 phút

@app.get("/")
def home():
    return {"message": "Stock API CLEAN (VCI Trading Core)"}

# --- 1. CORE LOGIC ---
def process_dataframe(df, source_type='trading'):
    if df is None or df.empty: return None
    
    # Chuẩn hóa tên cột
    df.columns = [col.lower() for col in df.columns]
    
    # Xử lý ngày tháng
    date_col = next((c for c in ['trading_date', 'time', 'date', 'ngay'] if c in df.columns), None)
    if not date_col: return None
    
    df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    df = df.sort_values('date')
    
    # MAPPING DỮ LIỆU
    if source_type == 'trading':
        df['close'] = df.get('close', 0.0)
        # Volume chính xác nhất từ Trading là matched_volume
        df['volume'] = df.get('matched_volume', df.get('volume', 0.0))
        # Mapping khối ngoại chuẩn VCI
        df['foreign_buy'] = df.get('fr_buy_volume_matched', 0.0)
        df['foreign_sell'] = df.get('fr_sell_volume_matched', 0.0)
    else: # Quote fallback
        df['close'] = df.get('close', 0.0)
        df['volume'] = df.get('volume', 0.0)
        df['foreign_buy'] = 0.0
        df['foreign_sell'] = 0.0

    df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']

    # Fix đơn vị giá (VCI trả về nghìn đồng nếu giá < 500)
    if not df.empty and df['close'].iloc[-1] < 500:
        for c in ['open', 'high', 'low', 'close']:
            if c in df.columns: df[c] = df[c] * 1000
            
    return df

def get_data_robust(symbol: str, start_date: str, end_date: str):
    # CÁCH 1: Dùng Trading (Có khối ngoại)
    try:
        try:
            trading = Trading(symbol=symbol, source='VCI')
            df = trading.price_history(start=start_date, end=end_date)
        except:
            trading = Trading(source='VCI')
            df = trading.price_history(symbol=symbol, start=start_date, end=end_date)
            
        if df is not None and not df.empty:
            return process_dataframe(df, 'trading'), None
    except Exception as e:
        print(f"Trading API Error: {e}")

    # CÁCH 2: Dùng Quote (Dự phòng)
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty:
            return process_dataframe(df, 'quote'), "Dữ liệu khối ngoại gián đoạn (Fallback mode)."
    except:
        pass

    return None, "Không lấy được dữ liệu."

# --- 2. API STOCK (MAIN) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        # Cache Check
        if symbol in STOCK_CACHE:
            if current_time - STOCK_CACHE[symbol]['timestamp'] < CACHE_DURATION:
                return STOCK_CACHE[symbol]['data']

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        df, warning = get_data_robust(symbol, start_date, end_date)
        
        if df is None: return {"error": warning}

        # Shark Logic
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        shark_action, shark_color = "Lưỡng lự", "neutral"
        if vol_ratio > 1.3:
            if price_change > 1.5: shark_action, shark_color = "Gom hàng mạnh", "strong_buy"
            elif price_change < -1.5: shark_action, shark_color = "Xả hàng mạnh", "strong_sell"
            else: shark_action, shark_color = "Biến động mạnh", "warning"

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
                "detail": f"Vol {vol_ratio:.1f}x, Giá {price_change:.1f}%",
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

# --- 3. API NEWS ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn)'
        rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=vi&gl=VN&ceid=VN:vi"
        feed = feedparser.parse(rss_url)
        return [{"title": e.title, "link": e.link, "publishdate": f"{e.published_parsed.tm_year}-{e.published_parsed.tm_mon:02d}-{e.published_parsed.tm_mday:02d}" if e.get("published_parsed") else "", "source": "Google"} for e in feed.entries[:10]]
    except: return []

# --- 4. API REALTIME ---
@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        trading = Trading(source='VCI')
        df = trading.price_board([symbol.upper()])
        return df.to_dict(orient='records') if df is not None else {"error": "No Data"}
    except Exception as e: return {"error": str(e)}
# --- 5. API CHỈ SỐ THỊ TRƯỜNG ---

@app.get("/api/index/{index_symbol}")
def get_index_data(index_symbol: str):
    try:
        index_symbol = index_symbol.upper()
        # Dùng hàm robust luôn cho chỉ số (VNINDEX, VN30...)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        df, _ = get_data_robust(index_symbol, start_date, end_date)
        if df is not None:
             return df.to_dict(orient='records')
        return {"error": "Không lấy được dữ liệu chỉ số"}
    except Exception as e:
        return {"error": str(e)}
        
# --- 5. API TOP MOVER ---
@app.get("/api/top_mover")
def get_top_mover(filter: str = 'ForeignTrading', limit: int = 10):
    try:
        if market_top_mover:
            df = market_top_mover(filter=filter, limit=limit)
            if df is not None: return df.to_dict(orient='records')
        return {"error": "Not Supported"}
    except: return {"error": "Error"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
