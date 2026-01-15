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
CACHE_DURATION = 300 # 5 phút

@app.get("/")
def home():
    return {"message": "Stock API (SSI Quote Priority)"}

# --- 1. CORE PROCESSING ---
def process_dataframe(df):
    if df is None or df.empty: return None
    
    # 1. Chuẩn hóa tên cột về chữ thường
    df.columns = [col.lower() for col in df.columns]
    
    # 2. Xử lý cột Ngày (SSI thường trả về format 'dd/mm/yyyy' hoặc 'yyyy-mm-dd')
    date_col = next((c for c in ['trading_date', 'time', 'date', 'ngay'] if c in df.columns), None)
    if not date_col: return None
    
    # Convert sang datetime rồi format chuẩn YYYY-MM-DD
    # try/except để xử lý nhiều format ngày khác nhau
    try:
        df['date'] = pd.to_datetime(df[date_col], dayfirst=True).dt.strftime('%Y-%m-%d')
    except:
        df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        
    df = df.sort_values('date')
    
    # 3. MAPPING CỘT (QUAN TRỌNG)
    # Cố gắng bắt tất cả các tên cột có thể xuất hiện từ SSI
    
    # Giá & Volume
    df['close'] = df.get('close', 0.0)
    df['open'] = df.get('open', 0.0)
    df['high'] = df.get('high', 0.0)
    df['low'] = df.get('low', 0.0)
    df['volume'] = df.get('volume', df.get('total_volume', 0.0))

    # Khối ngoại (SSI thường trả về cột 'foreign_buy', 'foreign_sell' hoặc 'buy_foreign_qtty'...)
    # Ta dùng hàm get với danh sách ưu tiên
    
    # Mua
    df['foreign_buy'] = df.get('foreign_buy', 
                        df.get('buy_foreign_quantity', 
                        df.get('buy_foreign_qtty', 
                        df.get('foreign_buy_volume', 0.0))))
    
    # Bán
    df['foreign_sell'] = df.get('foreign_sell', 
                         df.get('sell_foreign_quantity', 
                         df.get('sell_foreign_qtty', 
                         df.get('foreign_sell_volume', 0.0))))

    # Tính Ròng
    df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']

    # 4. Fix đơn vị giá (Nếu < 500 thì nhân 1000)
    if not df.empty and df['close'].iloc[-1] < 500:
        for c in ['open', 'high', 'low', 'close']:
            if c in df.columns: df[c] = df[c] * 1000
            
    return df

def get_data_robust(symbol: str, start_date: str, end_date: str):
    print(f"Fetching {symbol} ({start_date} -> {end_date})...")

    # --- CHIẾN THUẬT: Ưu tiên Quote(SSI) ---
    # SSI là nguồn Quote hiếm hoi thường kèm khối ngoại và ít bị chặn
    try:
        print("  Attempt 1: Quote(SSI)...")
        quote = Quote(symbol=symbol, source='SSI')
        df = quote.history(start=start_date, end=end_date, interval='1D')
        
        if df is not None and not df.empty:
            # Kiểm tra xem có cột khối ngoại không
            cols = [c.lower() for c in df.columns]
            has_foreign = any('foreign' in c for c in cols)
            
            print(f"  -> SSI Success! Rows: {len(df)}. Has Foreign columns: {has_foreign}")
            if not has_foreign:
                print(f"  -> Warning: SSI data found but columns are: {cols}")
                
            return process_dataframe(df), None
    except Exception as e:
        print(f"  -> SSI failed: {e}")

    # --- FALLBACK: Quote(VCI) ---
    # Nếu SSI lỗi, về lại VCI (chấp nhận mất khối ngoại để App sống)
    try:
        print("  Attempt 2: Fallback Quote(VCI)...")
        quote = Quote(symbol=symbol, source='VCI')
        df = quote.history(start=start_date, end=end_date, interval='1D')
        
        if df is not None and not df.empty:
            return process_dataframe(df), "Dữ liệu khối ngoại tạm thời gián đoạn (VCI Quote)."
    except Exception as e:
        print(f"  -> VCI failed: {e}")

    return None, "Không lấy được dữ liệu từ mọi nguồn."

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
