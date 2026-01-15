from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import vnstock as vnstock_lib
# Import Trading để dùng tính năng price_history của VCI
from vnstock import Quote, Trading, config
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import numpy as np
import time

# --- CONFIG ---
print("vnstock loaded from:", vnstock_lib.__file__)
if hasattr(config, 'proxy_enabled'):
    config.proxy_enabled = True # Vẫn bật để dự phòng

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GLOBAL CACHE (Vẫn giữ để tối ưu tốc độ) ---
STOCK_CACHE = {}
CACHE_DURATION = 300 # 5 phút

@app.get("/")
def home():
    return {"message": "Stock API VCI Ultimate (Price + Foreign in ONE request)"}

# --- HÀM LẤY DỮ LIỆU TỪ VCI (Trading Class) ---
def get_vci_full_data(symbol: str, start_date: str, end_date: str):
    print(f"Fetching VCI Full Data for {symbol}...")
    try:
        # Sử dụng Trading class của VCI theo tài liệu bạn gửi
        trading = Trading(source='VCI')
        
        # Hàm price_history trả về cả Giá và Khối ngoại
        df = trading.price_history(symbol=symbol, start=start_date, end=end_date)
        
        if df is None or df.empty:
            print("  VCI returned no data.")
            return None
            
        # --- CHUẨN HÓA DỮ LIỆU ---
        # 1. Chuyển tên cột về chữ thường
        df.columns = [col.lower() for col in df.columns]
        
        # 2. Xử lý ngày tháng (cột trading_date)
        if 'trading_date' in df.columns:
            df['date'] = pd.to_datetime(df['trading_date']).dt.strftime('%Y-%m-%d')
            df = df.sort_values('date')
        else:
            print("  Missing 'trading_date' column.")
            return None

        # 3. Map các cột quan trọng
        # Cột giá & volume cơ bản
        df['close'] = df['close']
        df['open'] = df['open']
        df['high'] = df['high']
        df['low'] = df['low']
        # Dùng matched_volume (khớp lệnh) làm volume chính
        df['volume'] = df.get('matched_volume', df.get('volume', 0))

        # 4. Map cột Khối ngoại (Từ tài liệu: fr_buy_volume_matched...)
        # Lưu ý: VCI trả về nhiều loại volume (matched, deal, total). Ta dùng matched (khớp lệnh trên sàn).
        df['foreign_buy'] = df.get('fr_buy_volume_matched', 0.0)
        df['foreign_sell'] = df.get('fr_sell_volume_matched', 0.0)
        
        # Tính Net
        df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']

        # Fix đơn vị giá (VCI thường trả về đơn vị 1000đ, ví dụ 64.5 -> cần nhân 1000 thành 64500)
        # Kiểm tra mẫu dữ liệu cuối cùng
        last_close = df['close'].iloc[-1]
        if last_close < 500: # Ngưỡng an toàn để phát hiện đơn vị nghìn
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = df[col] * 1000

        # --- TÍNH TOÁN SHARK & CHỈ SỐ PHỤ ---
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

        # Shark Logic
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = "Không tín hiệu"

        if vol_ratio > 1.3:
            if price_change > 1.5:
                shark_action, shark_color = "Gom hàng mạnh", "strong_buy"
            elif price_change < -1.5:
                shark_action, shark_color = "Xả hàng mạnh", "strong_sell"
            else:
                shark_action, shark_color = "Biến động mạnh", "warning"
        
        shark_detail = f"Vol {vol_ratio:.1f}x, Giá {price_change:.1f}%"

        # Output Format
        data_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'foreign_buy', 'foreign_sell', 'foreign_net', 'foreign_ratio']
        
        return {
            "data": df[data_cols].fillna(0).to_dict(orient='records'),
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

    except Exception as e:
        print(f"VCI Error: {e}")
        return None

# --- API ENDPOINTS ---

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        # 1. Cache Check
        if symbol in STOCK_CACHE:
            if current_time - STOCK_CACHE[symbol]['timestamp'] < CACHE_DURATION:
                return STOCK_CACHE[symbol]['data']
        
        # 2. Fetch Data (VCI Only)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        result = get_vci_full_data(symbol, start_date, end_date)
        
        if result:
            STOCK_CACHE[symbol] = {'timestamp': current_time, 'data': result}
            return result
        else:
            return {"error": "Không lấy được dữ liệu từ VCI"}

    except Exception as e:
        return {"error": str(e)}

@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    # Tận dụng luôn hàm get_stock (đã có VCI Foreign)
    data = get_stock(symbol)
    if "error" in data or "data" not in data: return []
    
    results = []
    for row in data['data']:
        results.append({
            "date": row['date'],
            "buyVol": row.get('foreign_buy', 0),
            "sellVol": row.get('foreign_sell', 0),
            "netVolume": row.get('foreign_net', 0)
        })
    return results[-90:] # Lấy 90 ngày

@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        trading = Trading(source='VCI')
        df = trading.price_board([symbol.upper()])
        if df is not None and not df.empty:
            return df.to_dict(orient='records')
        return {"error": "No Data"}
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
