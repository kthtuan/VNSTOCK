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
CACHE_DURATION = 300 # Cache 5 phút

@app.get("/")
def home():
    return {"message": "Stock API (VCI New Foreign Trade Function)"}

# --- 1. CORE LOGIC ---
def process_dataframe(df):
    if df is None or df.empty: return None
    df.columns = [col.lower() for col in df.columns]
    
    # Xử lý cột ngày (có thể là time, trading_date, date)
    date_col = next((c for c in ['time', 'trading_date', 'date', 'ngay'] if c in df.columns), None)
    if not date_col: return None
    
    try:
        df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    except:
        pass
            
    df = df.sort_values('date')
    return df

def get_data_vci_new(symbol: str, start_date: str, end_date: str):
    """
    Chiến thuật mới:
    1. Lấy lịch sử giá (Quote)
    2. Lấy lịch sử khối ngoại (Trading.foreign_trade) - Hàm mới!
    3. Merge lại với nhau
    """
    print(f"Fetching {symbol} VCI New Method...")
    
    # BƯỚC 1: LẤY GIÁ (QUOTE)
    try:
        quote = Quote(symbol=symbol, source='VCI')
        df_price = quote.history(start=start_date, end=end_date, interval='1D')
        df_price = process_dataframe(df_price)
        
        if df_price is None or df_price.empty:
            return None, "Không lấy được dữ liệu giá."
            
        # Chuẩn hóa cột giá
        df_price['close'] = df_price.get('close', 0.0)
        df_price['volume'] = df_price.get('volume', 0.0)
        # Fix đơn vị giá
        if df_price['close'].iloc[-1] < 500:
            for c in ['open', 'high', 'low', 'close']:
                if c in df_price.columns: df_price[c] *= 1000
                
    except Exception as e:
        print(f"Quote Error: {e}")
        return None, f"Lỗi lấy giá: {e}"

    # BƯỚC 2: LẤY KHỐI NGOẠI (HÀM MỚI foreign_trade)
    try:
        trading = Trading(source='VCI')
        # Lưu ý: Hàm foreign_trade() có thể không cần tham số hoặc cần symbol tùy version
        # Ta thử gọi an toàn
        try:
            # Cách gọi mới cho Trading VCI thường là khởi tạo Trading(symbol=...)
            trading_symbol = Trading(symbol=symbol, source='VCI')
            df_foreign = trading_symbol.foreign_trade() 
        except:
            # Fallback cách cũ
            df_foreign = trading.foreign_trade(symbol=symbol)
            
        df_foreign = process_dataframe(df_foreign)
        
        if df_foreign is not None and not df_foreign.empty:
            print(f"  -> Found Foreign Data: {len(df_foreign)} rows")
            # Map cột khối ngoại (tên cột có thể là buy_volume, sell_volume...)
            # Kiểm tra tên cột thực tế trả về để map
            
            # Giả định tên cột trả về từ foreign_trade
            # Thường là: date, buy_volume, sell_volume, buy_value, sell_value
            
            # Đổi tên để merge
            rename_map = {}
            for c in df_foreign.columns:
                if 'buy' in c and 'vol' in c: rename_map[c] = 'foreign_buy'
                if 'sell' in c and 'vol' in c: rename_map[c] = 'foreign_sell'
            
            df_foreign = df_foreign.rename(columns=rename_map)
            
            # Chỉ giữ lại các cột cần thiết để merge
            cols_to_merge = ['date']
            if 'foreign_buy' in df_foreign.columns: cols_to_merge.append('foreign_buy')
            if 'foreign_sell' in df_foreign.columns: cols_to_merge.append('foreign_sell')
            
            df_foreign = df_foreign[cols_to_merge]
            
            # MERGE: Left join vào bảng giá
            df_final = pd.merge(df_price, df_foreign, on='date', how='left')
            
            # Fill NaN bằng 0
            df_final['foreign_buy'] = df_final['foreign_buy'].fillna(0.0)
            df_final['foreign_sell'] = df_final['foreign_sell'].fillna(0.0)
            
            return df_final, None
        else:
            print("  -> No Foreign Data returned.")
            # Vẫn trả về bảng giá dù không có khối ngoại
            df_price['foreign_buy'] = 0.0
            df_price['foreign_sell'] = 0.0
            return df_price, "Không lấy được khối ngoại (Hàm mới trả về rỗng)."
            
    except Exception as e:
        print(f"Foreign Trade Error: {e}")
        # Vẫn trả về giá
        df_price['foreign_buy'] = 0.0
        df_price['foreign_sell'] = 0.0
        return df_price, f"Lỗi lấy khối ngoại: {str(e)}"

# --- 2. API ENDPOINTS ---
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
        
        # GỌI HÀM MỚI
        df, warning = get_data_vci_new(symbol, start_date, end_date)
        
        if df is None: return {"error": warning}

        # --- BƯỚC 3: TÍNH TOÁN SHARK ---
        # Đảm bảo các cột tồn tại
        if 'foreign_buy' not in df.columns: df['foreign_buy'] = 0.0
        if 'foreign_sell' not in df.columns: df['foreign_sell'] = 0.0
        
        df['foreign_net'] = df['foreign_buy'] - df['foreign_sell']
        
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last
        
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        # Shark Logic V2
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
        index_symbol = index_symbol.upper()
        # Dùng hàm mới lấy giá cho index luôn
        quote = Quote(symbol=index_symbol, source='VCI')
        df = quote.history(start=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'), 
                           end=datetime.now().strftime('%Y-%m-%d'), interval='1D')
        df = process_dataframe(df)
        if df is not None:
             return df.to_dict(orient='records')
        return {"error": "Không lấy được dữ liệu chỉ số"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
