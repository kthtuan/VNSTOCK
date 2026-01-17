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
import feedparser

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
    return {"message": "Stock API Final (VCI Quote + Realtime Patch)"}

# --- 1. DATA PROCESSING ---
def process_dataframe(df):
    if df is None or df.empty: return None
    df.columns = [col.lower() for col in df.columns]
    
    # Xử lý cột ngày
    date_col = next((c for c in ['time', 'trading_date', 'date', 'ngay'] if c in df.columns), None)
    if not date_col: return None
    
    try:
        df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    except:
        pass
            
    df = df.sort_values('date')
    
    # Đảm bảo có đủ cột và fill 0
    for col in ['close', 'volume', 'foreign_buy', 'foreign_sell']:
        if col not in df.columns: df[col] = 0.0
    
    df['foreign_buy'] = df['foreign_buy'].fillna(0.0)
    df['foreign_sell'] = df['foreign_sell'].fillna(0.0)
    df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']
    
    # Fix đơn vị giá (VCI trả về nghìn đồng nếu giá < 500)
    if not df.empty and df['close'].iloc[-1] < 500:
        for c in ['open', 'high', 'low', 'close']:
            if c in df.columns: df[c] = df[c] * 1000
            
    return df

def get_realtime_data(symbol: str):
    """Lấy dữ liệu realtime (Giá + Khối ngoại) từ bảng giá VCI"""
    try:
        trading = Trading(source='VCI')
        # Lấy bảng giá realtime (Hàm này vẫn chạy tốt trên Render)
        df = trading.price_board([symbol])
        
        if df is not None and not df.empty:
            row = df.iloc[0]
            
            # Mapping các cột khối ngoại từ Realtime Board
            # Tên cột có thể thay đổi tùy version vnstock, ta check hết các khả năng
            f_buy = float(row.get('foreign_buy_volume', row.get('foreign_buy_vol', row.get('buy_foreign_qtty', 0))))
            f_sell = float(row.get('foreign_sell_volume', row.get('foreign_sell_vol', row.get('sell_foreign_qtty', 0))))
            
            close = float(row.get('match_price', row.get('close', 0)))
            vol = float(row.get('total_volume', row.get('volume', 0)))
            
            # Chỉ trả về khi có dữ liệu giao dịch
            if vol > 0:
                return {
                    "foreign_buy": f_buy,
                    "foreign_sell": f_sell,
                    "close": close,
                    "volume": vol
                }
    except Exception as e:
        print(f"Realtime Error: {e}")
    return None

# --- 2. MAIN DATA FETCHING ---
def get_stock_data(symbol: str, start_date: str, end_date: str):
    # A. LẤY GIÁ TỪ VCI (QUOTE)
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df_price = quote.history(start=start_date, end=end_date, interval='1D')
        df = process_dataframe(df_price)
    except Exception as e:
        return None, f"Lỗi Quote: {e}"

    if df is None: return None, "Không lấy được dữ liệu lịch sử."

    # B. VÁ LỖI BẰNG REALTIME (Quan trọng)
    # Vì lịch sử Quote không có khối ngoại (do chặn IP hoặc nguồn VCI Quote không có)
    # Ta lấy khối ngoại hôm nay đắp vào.
    rt_data = get_realtime_data(symbol)
    warning = None
    
    if rt_data:
        today_str = datetime.now().strftime('%Y-%m-%d')
        last_date = df['date'].iloc[-1]
        
        # Trường hợp 1: Dữ liệu lịch sử đã có dòng hôm nay -> Update đè lên
        if last_date == today_str:
            idx = df.index[-1]
            df.at[idx, 'foreign_buy'] = rt_data['foreign_buy']
            df.at[idx, 'foreign_sell'] = rt_data['foreign_sell']
            df.at[idx, 'foreign_net'] = rt_data['foreign_buy'] - rt_data['foreign_sell']
            
            # Cập nhật giá/vol mới nhất từ bảng điện
            if rt_data['close'] > 0: df.at[idx, 'close'] = rt_data['close']
            if rt_data['volume'] > 0: df.at[idx, 'volume'] = rt_data['volume']
            
            warning = "Dữ liệu hôm nay được cập nhật từ Bảng điện (Realtime)."
            
        # Trường hợp 2: Lịch sử chưa cập nhật ngày hôm nay -> Thêm dòng mới
        elif last_date < today_str:
            new_row = df.iloc[-1].copy()
            new_row['date'] = today_str
            new_row['close'] = rt_data['close']
            new_row['volume'] = rt_data['volume']
            new_row['foreign_buy'] = rt_data['foreign_buy']
            new_row['foreign_sell'] = rt_data['foreign_sell']
            new_row['foreign_net'] = rt_data['foreign_buy'] - rt_data['foreign_sell']
            
            # Các cột OHL tạm lấy bằng giá Close realtime
            new_row['open'] = rt_data['close']
            new_row['high'] = rt_data['close']
            new_row['low'] = rt_data['close']
            
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            warning = "Đã bổ sung dữ liệu ngày hôm nay từ Realtime."
            
    return df, warning

# --- API ENDPOINTS ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        # Check Cache
        if symbol in STOCK_CACHE:
            if current_time - STOCK_CACHE[symbol]['timestamp'] < CACHE_DURATION:
                return STOCK_CACHE[symbol]['data']

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # GỌI HÀM
        df, warning = get_stock_data(symbol, start_date, end_date)
        
        if df is None: return {"error": warning}

        # --- TÍNH TOÁN SHARK ---
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        # Shark Analysis Logic V2
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        
        IS_VOL_SPIKE = vol_ratio > 1.3
        IS_PRICE_UP = price_change > 2.0
        IS_PRICE_DOWN = price_change < -2.0
        IS_FOREIGN_BUY = last['foreign_net'] > 0
        IS_FOREIGN_SELL = last['foreign_net'] < 0
        
        if IS_VOL_SPIKE:
            if IS_PRICE_UP:
                if IS_FOREIGN_BUY: shark_action, shark_color = "Gom hàng mạnh (Uy tín)", "strong_buy"
                elif IS_FOREIGN_SELL: shark_action, shark_color = "Coi chừng Kéo Xả (FOMO)", "warning"
                else: shark_action, shark_color = "Dòng tiền đầu cơ nóng", "buy"
            elif IS_PRICE_DOWN:
                if IS_FOREIGN_BUY: shark_action, shark_color = "Đè gom (Hoảng loạn)", "buy"
                else: shark_action, shark_color = "Xả hàng mạnh", "strong_sell"
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
        quote = Quote(symbol=index_symbol.upper(), source='VCI')
        df = quote.history(start=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'), 
                           end=datetime.now().strftime('%Y-%m-%d'), interval='1D')
        if df is not None:
             df = process_dataframe(df)
             return df.to_dict(orient='records')
        return {"error": "No Data"}
    except Exception as e: return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
