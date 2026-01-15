from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Import module gốc dưới tên khác để truy cập __file__
import vnstock as vnstock_lib
# Import các Class chức năng theo chuẩn mới
from vnstock import Quote, Listing, Company, Finance, Trading, Screener, config
# Thử import các hàm tiện ích cũ nếu còn hỗ trợ
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

@app.get("/")
def home():
    return {"message": "Stock API Hybrid (VCI Price + TCBS Foreign)"}

# --- 1. HÀM CHUẨN HÓA DATAFRAME ---
def normalize_dataframe(df):
    """Chuẩn hóa tên cột và định dạng ngày tháng để merge"""
    if df is None or df.empty:
        return None
    
    # 1. Lowercase & snake_case tên cột
    df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
    
    # 2. Xử lý cột Date về chuẩn YYYY-MM-DD
    date_col = next((c for c in ['time', 'tradingdate', 'date', 'ngay'] if c in df.columns), None)
    if date_col:
        # Chuyển về datetime objects trước
        df['date_obj'] = pd.to_datetime(df[date_col])
        # Tạo cột string để merge chuẩn xác
        df['date_str'] = df['date_obj'].dt.strftime('%Y-%m-%d')
        # Sắp xếp theo ngày tăng dần
        df = df.sort_values('date_str')
    
    return df

# --- 2. HÀM FETCH ĐƠN LẺ ---
def fetch_source(symbol, start, end, source):
    """Hàm lấy dữ liệu từ 1 nguồn cụ thể"""
    print(f"  → Requesting {source}...")
    try:
        quote = Quote(symbol=symbol, source=source)
        df = quote.history(start=start, end=end, interval='1D')
        if df is not None and not df.empty:
            return normalize_dataframe(df)
    except Exception as e:
        print(f"    {source} Error: {e}")
    return None

# --- 3. HÀM HYBRID THÔNG MINH ---
def get_stock_data_hybrid(symbol: str, start_date: str, end_date: str):
    print(f"Fetching Hybrid for {symbol}: VCI (Price) + TCBS (Foreign)")
    
    # BƯỚC 1: Lấy dữ liệu Giá từ VCI (Ưu tiên tốc độ)
    df_price = fetch_source(symbol, start_date, end_date, 'VCI')
    
    if df_price is None or df_price.empty:
        print("  VCI failed. Trying TCBS as fallback for price...")
        df_price = fetch_source(symbol, start_date, end_date, 'TCBS')
        if df_price is None or df_price.empty:
            return None # Cả 2 đều tạch

    # BƯỚC 2: Lấy dữ liệu Khối ngoại từ TCBS
    # (Chỉ lấy nếu VCI thành công nhưng thiếu cột khối ngoại)
    
    # Kiểm tra xem VCI đã có khối ngoại chưa (thường là chưa)
    has_foreign = any(c in df_price.columns for c in ['foreign_buy', 'nn_mua', 'buy_foreign_volume'])
    
    if not has_foreign:
        print("  VCI lacks foreign data. Fetching TCBS for enrichment...")
        try:
            # Lấy TCBS (Cho phép fail mà không chết app)
            df_foreign = fetch_source(symbol, start_date, end_date, 'TCBS')
            
            if df_foreign is not None and not df_foreign.empty:
                print(f"  Merging TCBS foreign data ({len(df_foreign)} rows)...")
                
                # Chọn các cột khối ngoại cần thiết từ TCBS
                foreign_cols = [c for c in df_foreign.columns if any(k in c for k in ['foreign', 'nn_', 'buy', 'sell'])]
                
                # Thêm cột nối 'date_str'
                cols_to_merge = ['date_str'] + foreign_cols
                # Lọc chỉ lấy cột có trong df_foreign
                cols_to_merge = [c for c in cols_to_merge if c in df_foreign.columns]
                
                # MERGE: Nối dữ liệu khối ngoại vào bảng giá dựa trên ngày
                df_merged = pd.merge(
                    df_price, 
                    df_foreign[cols_to_merge], 
                    on='date_str', 
                    how='left', 
                    suffixes=('', '_tcbs')
                )
                
                # Điền 0 vào các ô khối ngoại bị null (do lệch ngày)
                for col in foreign_cols:
                    if col in df_merged.columns:
                        df_merged[col] = df_merged[col].fillna(0)
                
                df_price = df_merged
                print("  Merge success!")
            else:
                print("  TCBS returned no data. Skipping foreign flow.")
        except Exception as e:
            print(f"  Hybrid Merge Failed: {e}. Returning Price only.")
    
    return df_price

# --- 4. API STOCK (CORE) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        # GỌI HÀM HYBRID
        df = get_stock_data_hybrid(symbol, start_date, end_date)

        if df is None or df.empty:
            return {"error": "Không lấy được dữ liệu"}

        # Chuẩn bị cột Date để hiển thị
        df['date'] = df['date_str'] # Đã có sẵn từ hàm normalize

        # Fix đơn vị giá (nhân 1000 nếu < 500)
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
            for c in ['open', 'high', 'low', 'close']:
                if c in df.columns:
                    df[c] *= 1000

        # Map cột khối ngoại (Xử lý các tên khác nhau sau khi merge)
        # Ưu tiên các cột từ TCBS (thường có hậu tố hoặc tên chuẩn)
        df['foreign_buy'] = 0.0
        df['foreign_sell'] = 0.0
        df['foreign_net'] = 0.0
        
        # Danh sách cột tiềm năng (bao gồm cả cột gốc và cột merged)
        buy_cols = ['buy_foreign_volume', 'buy_foreign_qtty', 'nn_mua', 'foreign_buy']
        sell_cols = ['sell_foreign_volume', 'sell_foreign_qtty', 'nn_ban', 'foreign_sell']
        
        for idx, row in df.iterrows():
            # Tìm Value Buy
            for c in buy_cols:
                if c in row and pd.notna(row[c]):
                    df.at[idx, 'foreign_buy'] = float(row[c])
                    break
            # Tìm Value Sell
            for c in sell_cols:
                if c in row and pd.notna(row[c]):
                    df.at[idx, 'foreign_sell'] = float(row[c])
                    break
            # Tính Net
            df.at[idx, 'foreign_net'] = df.at[idx, 'foreign_buy'] - df.at[idx, 'foreign_sell']

        # Tính chỉ số phụ
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        # Shark Logic
        vol_ratio = last['volume'] / (last['ma20_vol'] if last['ma20_vol'] > 0 else 1)
        price_change = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] > 0 else 0
        
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = "Không tín hiệu"

        if vol_ratio > 1.3:
            if price_change > 1.5:
                shark_action = "Gom hàng mạnh"
                shark_color = "strong_buy"
                shark_detail = f"Vol nổ {vol_ratio:.1f}x + Giá tăng {price_change:.1f}%"
            elif price_change < -1.5:
                shark_action = "Xả hàng mạnh"
                shark_color = "strong_sell"
                shark_detail = f"Vol nổ {vol_ratio:.1f}x + Giá giảm {price_change:.1f}%"
            else:
                shark_action = "Biến động mạnh"
                shark_color = "warning"
                shark_detail = f"Vol cao {vol_ratio:.1f}x nhưng giá sideway"
        elif vol_ratio < 0.6 and abs(price_change) > 2:
             if price_change > 2:
                shark_action = "Kéo giá (tiết cung)"
                shark_color = "buy"
             elif price_change < -2:
                shark_action = "Đè giá (cạn vol)"
                shark_color = "sell"

        # Warning
        warning = None
        if last['foreign_net'] == 0 and last['cum_net_5d'] == 0:
            warning = "Dữ liệu khối ngoại không khả dụng hoặc bằng 0 (chế độ Hybrid VCI)."

        # Response
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
            },
            "warning": warning
        }

    except Exception as e:
        print(f"API Error {symbol}: {e}")
        return {"error": str(e)}

# --- 5. API FOREIGN RIÊNG (DÙNG CHO BIỂU ĐỒ FLOW) ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    # Với API này, ta chỉ cần TCBS thôi cho lẹ, không cần VCI
    try:
        symbol = symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        # Chỉ gọi TCBS
        df = fetch_source(symbol, start_date, end_date, 'TCBS')
        
        if df is None or df.empty:
            # Fallback sang Hybrid nếu TCBS fail hoàn toàn
            df = get_stock_data_hybrid(symbol, start_date, end_date)
            
        if df is None or df.empty: return []

        # Logic map dữ liệu như cũ
        df['date'] = df['date_str']
        results = []
        # (Giữ nguyên logic loop cũ để map buy/sell/net)
        # ... Viết ngắn gọn lại:
        for _, row in df.iterrows():
            buy = next((float(row[c]) for c in ['buy_foreign_volume', 'nn_mua', 'foreign_buy'] if c in row and pd.notna(row[c])), 0.0)
            sell = next((float(row[c]) for c in ['sell_foreign_volume', 'nn_ban', 'foreign_sell'] if c in row and pd.notna(row[c])), 0.0)
            net = buy - sell
            results.append({"date": row['date'], "buyVol": buy, "sellVol": sell, "netVolume": net})
            
        return results
    except Exception as e:
        return []

# --- 6. API REALTIME ---
@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        # Dùng VCI cho bảng giá realtime
        trading = Trading(source='VCI')
        df = trading.price_board([symbol.upper()])
        if df is not None and not df.empty:
            return df.to_dict(orient='records')
        return {"error": "No Data"}
    except Exception as e:
        return {"error": str(e)}

# --- 7. API NEWS (Giữ nguyên) ---
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
