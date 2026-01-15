from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import vnstock as vnstock_lib
from vnstock import Quote, Listing, Company, Finance, Trading, Screener, config
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
import random

# --- CONFIG & DEBUG ---
print("vnstock loaded from:", vnstock_lib.__file__)

# Kiểm tra config proxy an toàn
if hasattr(config, 'proxy_enabled'):
    config.proxy_enabled = True
    print("Proxy enabled:", config.proxy_enabled)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GLOBAL CACHE (Giải pháp 3) ---
# Lưu trữ dữ liệu trong RAM để giảm tần suất gọi API
# Cấu trúc: { 'GAS': { 'timestamp': 17000..., 'data': {...} } }
STOCK_CACHE = {}
CACHE_DURATION = 300  # Cache tồn tại trong 300 giây (5 phút)

@app.get("/")
def home():
    return {"message": "Stock API Hybrid (VCI Price + TCBS/SSI Foreign + Caching)"}

# --- 1. HÀM CHUẨN HÓA DATAFRAME ---
def normalize_dataframe(df):
    if df is None or df.empty:
        return None
    df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
    date_col = next((c for c in ['time', 'tradingdate', 'date', 'ngay'] if c in df.columns), None)
    if date_col:
        df['date_obj'] = pd.to_datetime(df[date_col])
        df['date_str'] = df['date_obj'].dt.strftime('%Y-%m-%d')
        df = df.sort_values('date_str')
    return df

# --- 2. HÀM FETCH ĐƠN LẺ ---
def fetch_source(symbol, start, end, source):
    print(f"    → Requesting {source}...")
    try:
        # Quote init có thể thêm headers nếu cần thiết cho SSI/TCBS sau này
        quote = Quote(symbol=symbol, source=source)
        df = quote.history(start=start, end=end, interval='1D')
        if df is not None and not df.empty:
            return normalize_dataframe(df)
    except Exception as e:
        print(f"      {source} Error: {str(e)[:100]}...") # Log ngắn gọn lỗi
    return None

# --- 3. HÀM HYBRID THÔNG MINH (Giải pháp 2: Thêm SSI) ---
def get_stock_data_hybrid_logic(symbol: str, start_date: str, end_date: str):
    print(f"Fetching Hybrid Logic for {symbol}...")
    
    # BƯỚC 1: Lấy dữ liệu Giá từ VCI (Nhanh, ổn định)
    df_price = fetch_source(symbol, start_date, end_date, 'VCI')
    
    # Fallback giá nếu VCI lỗi
    if df_price is None or df_price.empty:
        print("  VCI failed for Price. Trying TCBS...")
        df_price = fetch_source(symbol, start_date, end_date, 'TCBS')
        if df_price is None or df_price.empty:
            return None

    # BƯỚC 2: Lấy dữ liệu Khối ngoại (Foreign)
    # Kiểm tra xem dữ liệu giá đã có khối ngoại chưa
    has_foreign = any(c in df_price.columns for c in ['foreign_buy', 'nn_mua', 'buy_foreign_volume'])
    
    if not has_foreign:
        print("  Enriching Foreign Data...")
        df_foreign = None
        
        # Chiến thuật Fallback cho Khối ngoại: TCBS -> SSI
        foreign_sources = ['TCBS', 'SSI']
        
        for src in foreign_sources:
            df_foreign = fetch_source(symbol, start_date, end_date, src)
            if df_foreign is not None and not df_foreign.empty:
                print(f"  Found foreign data from {src}")
                break # Đã lấy được, thoát vòng lặp
            else:
                print(f"  {src} returned no foreign data or blocked.")
        
        # Merge nếu lấy được dữ liệu khối ngoại
        if df_foreign is not None and not df_foreign.empty:
            try:
                # Lấy tất cả cột có vẻ là dữ liệu nước ngoài
                foreign_cols = [c for c in df_foreign.columns if any(k in c for k in ['foreign', 'nn_', 'buy', 'sell'])]
                cols_to_merge = ['date_str'] + [c for c in foreign_cols if c in df_foreign.columns]
                
                # Merge vào bảng giá gốc
                df_merged = pd.merge(
                    df_price, 
                    df_foreign[cols_to_merge], 
                    on='date_str', 
                    how='left', 
                    suffixes=('', '_ext')
                )
                
                # Fill 0 cho những ngày không có dữ liệu khớp
                for col in foreign_cols:
                    if col in df_merged.columns:
                        df_merged[col] = df_merged[col].fillna(0)
                
                df_price = df_merged
                print("  Merge success!")
            except Exception as e:
                print(f"  Merge error: {e}")
        else:
            print("  Warning: All foreign sources failed. Returning Price only.")

    # BƯỚC 3: Xử lý & Tính toán chỉ số (Shark, Ratio...)
    # Chuẩn bị tên cột chuẩn
    df = df_price
    df['date'] = df['date_str']

    # Fix đơn vị giá
    if 'close' in df.columns and df['close'].iloc[-1] < 500:
        for c in ['open', 'high', 'low', 'close']:
            if c in df.columns: df[c] *= 1000

    # Map cột khối ngoại về tên chuẩn (foreign_buy/sell/net)
    df['foreign_buy'] = 0.0
    df['foreign_sell'] = 0.0
    df['foreign_net'] = 0.0
    
    # Danh sách các tên cột tiềm năng từ các nguồn khác nhau
    buy_cols = ['buy_foreign_volume', 'buy_foreign_qtty', 'nn_mua', 'foreign_buy', 'foreign_buy_vol']
    sell_cols = ['sell_foreign_volume', 'sell_foreign_qtty', 'nn_ban', 'foreign_sell', 'foreign_sell_vol']
    
    for idx, row in df.iterrows():
        # Tìm giá trị mua
        for c in buy_cols:
            if c in row and pd.notna(row[c]):
                df.at[idx, 'foreign_buy'] = float(row[c])
                break
        # Tìm giá trị bán
        for c in sell_cols:
            if c in row and pd.notna(row[c]):
                df.at[idx, 'foreign_sell'] = float(row[c])
                break
        # Tính ròng
        df.at[idx, 'foreign_net'] = df.at[idx, 'foreign_buy'] - df.at[idx, 'foreign_sell']

    # Các chỉ số phụ
    df['volume'] = df['volume'].fillna(0).astype(float)
    df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
    df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

    # Shark Logic
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    
    vol_avg = last['ma20_vol'] if last['ma20_vol'] > 0 else 1
    vol_ratio = last['volume'] / vol_avg
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

    # Warning text
    warning = None
    if last['foreign_net'] == 0 and last['cum_net_5d'] == 0:
        warning = "Không lấy được dữ liệu khối ngoại (TCBS & SSI đều bị chặn hoặc không có số liệu)."

    # Format Output
    data_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'foreign_buy', 'foreign_sell', 'foreign_net']
    
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
        },
        "warning": warning
    }

# --- 4. API STOCK (CÓ CACHE) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        current_time = time.time()
        
        # 1. KIỂM TRA CACHE
        if symbol in STOCK_CACHE:
            cached_data = STOCK_CACHE[symbol]
            # Nếu cache chưa hết hạn (chưa quá 5 phút)
            if current_time - cached_data['timestamp'] < CACHE_DURATION:
                print(f"⚡ Returning CACHED data for {symbol}")
                return cached_data['data']
        
        # 2. NẾU KHÔNG CÓ CACHE -> GỌI HÀM XỬ LÝ
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        result = get_stock_data_hybrid_logic(symbol, start_date, end_date)
        
        if result is None:
            return {"error": "Không lấy được dữ liệu từ mọi nguồn"}

        # 3. LƯU VÀO CACHE
        STOCK_CACHE[symbol] = {
            'timestamp': current_time,
            'data': result
        }
        
        return result

    except Exception as e:
        print(f"API Error {symbol}: {e}")
        return {"error": str(e)}

# --- 5. API FOREIGN RIÊNG (CŨNG DÙNG CACHE) ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    # Tận dụng luôn hàm get_stock để hưởng lợi từ Cache và Logic Hybrid
    # Dữ liệu foreign đã có sẵn trong response của get_stock
    data = get_stock(symbol)
    
    if "error" in data or "data" not in data:
        return []
        
    results = []
    for row in data['data']:
        # Chỉ lấy 90 ngày gần nhất
        results.append({
            "date": row['date'],
            "buyVol": row.get('foreign_buy', 0),
            "sellVol": row.get('foreign_sell', 0),
            "netVolume": row.get('foreign_net', 0)
        })
    
    # Lấy 90 dòng cuối cùng
    return results[-90:]

# --- 6. API REALTIME (KHÔNG CACHE ĐỂ LẤY GIÁ MỚI NHẤT) ---
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

# --- 7. API NEWS ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn)'
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=vi&gl=VN&ceid=VN:vi"
        feed = feedparser.parse(rss_url)
        news_list = []
        for entry in feed.entries[:10]:
            d = entry.get("published_parsed")
            date_str = f"{d.tm_year}-{d.tm_mon:02d}-{d.tm_mday:02d}" if d else ""
            news_list.append({"title": entry.title, "link": entry.link, "publishdate": date_str, "source": "Google"})
        return news_list
    except: return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
