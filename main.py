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

# SỬA LỖI 1: Gọi __file__ từ module thư viện
print("vnstock loaded from:", vnstock_lib.__file__)

# SỬA LỖI 2: Kiểm tra config.proxy_enabled an toàn trước khi gọi
# Tránh lỗi AttributeError nếu phiên bản vnstock không có thuộc tính này
if hasattr(config, 'proxy_enabled'):
    print("Proxy enabled (current):", config.proxy_enabled)
    config.proxy_enabled = True
    print("Proxy enabled (set to):", config.proxy_enabled)
else:
    print("Note: config.proxy_enabled not available in this vnstock version. Skipping global proxy config.")

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
    
    # Ưu tiên nguồn dựa trên nhu cầu
    sources = ['TCBS', 'MSN', 'VCI'] if prefer_foreign else ['VCI', 'TCBS', 'MSN']
    
    df = None
    for src in sources:
        print(f"→ Trying source: {src}")
        max_attempts = 4 if src == 'TCBS' else 2
        for attempt in range(1, max_attempts + 1):
            print(f"  Attempt {attempt}/{max_attempts}")
            try:
                # Khởi tạo Quote. 
                # Lưu ý: Nếu phiên bản vnstock cũ không hỗ trợ tham số proxy=True trong constructor, 
                # bạn có thể cần xóa 'proxy=True' ở dòng dưới. 
                # Tuy nhiên, theo log cũ của bạn thì Quote có hỗ trợ.
                quote = Quote(symbol=symbol, source=src)
                print(f"  Quote init OK for {src}")
                
                # Lấy dữ liệu lịch sử
                df = quote.history(start=start_date, end=end_date, interval='1D')
                
                if df is not None and not df.empty:
                    print(f"  SUCCESS - {src} (attempt {attempt}) - Rows: {len(df)}")
                    # print(f"  Columns: {list(df.columns)}") # Uncomment để debug cột
                    break # Thoát vòng lặp attempt
                else:
                    print(f"  No data (df None or empty)")
            
            except Exception as e:
                err_str = str(e)
                print(f"  FAILED: {err_str}")
                # Retry logic
                if attempt < max_attempts and any(kw in err_str for kw in ['Connection', 'Timeout', 'RetryError']):
                    sleep_time = random.uniform(5, 15)
                    print(f"  Retry after {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                continue
        
        # Nếu đã có dữ liệu từ source này thì thoát vòng lặp source
        if df is not None and not df.empty:
            break
    
    if df is None or df.empty:
        print("→ All Quote sources failed.")
    
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

        # Chuẩn hóa tên cột
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Xử lý cột ngày tháng
        date_col = next((c for c in ['time', 'tradingdate', 'date', 'ngay'] if c in df.columns), None)
        if date_col:
            df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        elif hasattr(df.index, 'name') and df.index.name == 'time':
            df['date'] = df.index.strftime('%Y-%m-%d')
        else:
            df['date'] = ''

        # Xử lý đơn vị giá
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
            for c in ['open', 'high', 'low', 'close']:
                if c in df.columns:
                    df[c] *= 1000

        # Map các cột nước ngoài
        foreign_buy_candidates = ['foreign_buy', 'nn_mua', 'buy_foreign_volume', 'buy_foreign_qtty', 'nn_buy_vol', 'foreign_buy_vol']
        foreign_sell_candidates = ['foreign_sell', 'nn_ban', 'sell_foreign_volume', 'sell_foreign_qtty', 'nn_sell_vol', 'foreign_sell_vol']
        foreign_net_candidates = ['net_foreign_volume', 'nn_net_vol', 'khoi_ngoai_rong', 'net_foreign', 'foreign_net_vol', 'net_value']

        df['foreign_buy'] = 0.0
        df['foreign_sell'] = 0.0
        df['foreign_net'] = 0.0

        for idx, row in df.iterrows():
            # Lấy Buy
            for col in foreign_buy_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_buy'] = float(row[col])
                    break
            # Lấy Sell
            for col in foreign_sell_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_sell'] = float(row[col])
                    break
            
            # Tính Net
            df.at[idx, 'foreign_net'] = df.at[idx, 'foreign_buy'] - df.at[idx, 'foreign_sell']
            
            # Nếu Net = 0 (do thiếu buy/sell), thử tìm cột Net trực tiếp
            if df.at[idx, 'foreign_net'] == 0:
                for col in foreign_net_candidates:
                    if col in df.columns and pd.notna(row[col]):
                        df.at[idx, 'foreign_net'] = float(row[col])
                        break

        # Tính toán chỉ số phụ
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

        # Lấy dữ liệu phiên cuối
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        vol_current = last['volume']
        vol_avg = last['ma20_vol'] if not pd.isna(last['ma20_vol']) else 1
        vol_ratio = vol_current / vol_avg

        price_change_pct = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] != 0 else 0

        foreign_net_today = last['foreign_net']
        cum_net_5d = last['cum_net_5d']
        foreign_ratio_today = last['foreign_ratio']

        # Logic Shark Analysis
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = "Không có tín hiệu rõ ràng"

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
            warning = "Dữ liệu khối ngoại không khả dụng. Shark chỉ dựa trên volume/price."

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

# --- 5. API CHỈ SỐ THỊ TRƯỜNG ---
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

# --- 6. API TOP MOVER ---
@app.get("/api/top_mover")
def get_top_mover(filter: str = 'ForeignTrading', limit: int = 10):
    try:
        if market_top_mover:
            df = market_top_mover(filter=filter, limit=limit)
            if df is not None and not df.empty:
                print(f"Top mover for {filter} - Rows: {len(df)}")
                return df.to_dict(orient='records')
        
        return {"error": "Chức năng top mover không khả dụng trong phiên bản này"}
            
    except Exception as e:
        print(f"Top Mover Error {filter}: {e}")
        return {"error": str(e)}

# --- 7. API REALTIME ---
@app.get("/api/realtime/{symbol}")
def get_realtime(symbol: str):
    try:
        symbol = symbol.upper()
        # SỬA LỖI: Sử dụng Trading class thay vì hàm price_board() độc lập
        trading = Trading(source='VCI') 
        df = trading.price_board([symbol])
        
        if df is not None and not df.empty:
            print(f"Realtime data for {symbol}")
            return df.to_dict(orient='records')
        else:
            return {"error": "Không lấy được dữ liệu realtime"}
    except Exception as e:
        print(f"Realtime Error {symbol}: {e}")
        return {"error": f"Lỗi Realtime: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
