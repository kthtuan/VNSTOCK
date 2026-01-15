from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import Vnstock, Quote 
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import numpy as np
import time
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

# --- 1. HÀM HELPER: LẤY DỮ LIỆU AN TOÀN (Ưu tiên nguồn có foreign) ---
def get_stock_data_safe(symbol: str, start_date: str, end_date: str, prefer_foreign: bool = True):
    # Danh sách nguồn hợp lệ hiện tại (loại SSI, ưu tiên TCBS vì có foreign tốt)
    sources = ['TCBS', 'DNSE', 'VCI'] if prefer_foreign else ['VCI', 'TCBS', 'DNSE']
    
    for src in sources:
        for attempt in range(1, 3):  # Thử tối đa 2 lần (chủ yếu cho TCBS connection)
            try:
                # Khởi tạo Quote đúng cách: chỉ symbol + source
                quote = Quote(symbol=symbol, source=src)
                
                # Gọi history với tham số đúng
                df = quote.history(start=start_date, end=end_date, interval='1D')
                
                if df is not None and not df.empty:
                    print(f"Data from {src} (attempt {attempt}) for {symbol}")
                    return df
                
            except Exception as e:
                print(f"Source {src} attempt {attempt} failed for {symbol}: {e}")
                if attempt < 2 and 'ConnectionError' in str(e):
                    time.sleep(2)  # Chờ 2 giây rồi thử lại
                continue  # Thử attempt tiếp theo
            
            # Nếu không phải connection error → bỏ qua source này
            break

    # Fallback Vnstock cổ điển (VCI) nếu Quote fail hết
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty:
            print(f"VCI fallback success for {symbol}")
            return df
    except Exception as e:
        print(f"VCI fallback failed for {symbol}: {e}")

    print(f"All sources failed for {symbol}")
    return None

# --- 2. API STOCK (FULL: OHLCV + Foreign + Shark Analysis nâng cao) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        symbol = symbol.upper()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = get_stock_data_safe(symbol, start_date, end_date, prefer_foreign=True)

        if df is None or df.empty:
            return {"error": "Không lấy được dữ liệu"}

        # Chuẩn hóa cột: lowercase & replace space/_ 
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Xử lý date
        date_col = next((c for c in ['time', 'tradingdate', 'date', 'ngay'] if c in df.columns), None)
        if date_col:
            df['date'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
        else:
            df['date'] = df.index.strftime('%Y-%m-%d') if df.index.name == 'time' else ''

        # Fix giá nếu đơn vị sai (thường <500 là *1000)
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
            for c in ['open', 'high', 'low', 'close']:
                if c in df.columns:
                    df[c] *= 1000

        # Map foreign columns (rất nhiều tên khác nhau tùy nguồn)
        foreign_buy_candidates = ['foreign_buy', 'nn_mua', 'buy_foreign_volume', 'buy_foreign_qtty', 'nn_buy_vol', 'foreign_buy_vol']
        foreign_sell_candidates = ['foreign_sell', 'nn_ban', 'sell_foreign_volume', 'sell_foreign_qtty', 'nn_sell_vol', 'foreign_sell_vol']
        foreign_net_candidates = ['net_foreign_volume', 'nn_net_vol', 'khoi_ngoai_rong', 'net_foreign', 'foreign_net_vol', 'net_value']

        df['foreign_buy'] = 0.0
        df['foreign_sell'] = 0.0
        df['foreign_net'] = 0.0

        for idx, row in df.iterrows():
            # Buy
            for col in foreign_buy_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_buy'] = float(row[col])
                    break
            # Sell
            for col in foreign_sell_candidates:
                if col in df.columns and pd.notna(row[col]):
                    df.at[idx, 'foreign_sell'] = float(row[col])
                    break
            # Net fallback
            df.at[idx, 'foreign_net'] = df.at[idx, 'foreign_buy'] - df.at[idx, 'foreign_sell']
            if df.at[idx, 'foreign_net'] == 0:
                for col in foreign_net_candidates:
                    if col in df.columns and pd.notna(row[col]):
                        df.at[idx, 'foreign_net'] = float(row[col])
                        break

        # Tính thêm indicators cho shark
        df['volume'] = df['volume'].fillna(0).astype(float)
        df['ma20_vol'] = df['volume'].rolling(window=20, min_periods=1).mean()
        df['foreign_ratio'] = np.where(df['volume'] > 0, (df['foreign_buy'] + df['foreign_sell']) / df['volume'], 0)
        df['cum_net_5d'] = df['foreign_net'].rolling(window=5, min_periods=1).sum()

        # Dữ liệu mới nhất
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        vol_current = last['volume']
        vol_avg = last['ma20_vol'] if not pd.isna(last['ma20_vol']) else 1
        vol_ratio = vol_current / vol_avg

        price_change_pct = ((last['close'] - prev['close']) / prev['close'] * 100) if prev['close'] != 0 else 0

        foreign_net_today = last['foreign_net']
        cum_net_5d = last['cum_net_5d']
        foreign_ratio_today = last['foreign_ratio']

        # === SHARK ANALYSIS NÂNG CAO ===
        shark_action = "Lưỡng lự"
        shark_color = "neutral"
        shark_detail = "Không có tín hiệu rõ ràng"

        if vol_ratio > 1.5:
            if price_change_pct > 1.5 and foreign_net_today > 0:
                shark_action = "Cá mập ngoại GOM mạnh"
                shark_color = "strong_buy"
                shark_detail = f"Vol nổ {vol_ratio:.1f}x + Net ngoại +{foreign_net_today:,.0f:,}"
            elif price_change_pct < -1.5 and foreign_net_today < 0:
                shark_action = "Cá mập ngoại XẢ mạnh"
                shark_color = "strong_sell"
                shark_detail = f"Vol nổ {vol_ratio:.1f}x + Net ngoại {foreign_net_today:,.0f}"
            elif foreign_net_today > 100_000:
                shark_action = "Ngoại mua chủ động"
                shark_color = "buy"
            else:
                shark_action = "Biến động mạnh (có thể cá mập)"
                shark_color = "warning"

        elif vol_ratio < 0.7 and abs(price_change_pct) > 2:
            if price_change_pct > 2 and cum_net_5d > 0:
                shark_action = "Kéo giá nhẹ - Ngoại tích lũy"
                shark_color = "buy"
            elif price_change_pct < -2 and cum_net_5d < 0:
                shark_action = "Đè giá - Ngoại xả dần"
                shark_color = "sell"

        elif cum_net_5d > 500_000 and foreign_ratio_today > 0.2:
            shark_action = "Tích lũy ngoại dài hạn"
            shark_color = "buy"
            shark_detail = f"Cum net 5 ngày: +{cum_net_5d:,.0f}"

        # === RESPONSE ===
        data_cols = ['date', 'open', 'high', 'low', 'close', 'volume',
                     'foreign_buy', 'foreign_sell', 'foreign_net', 'foreign_ratio']

        return {
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

    except Exception as e:
        print(f"Stock Error {symbol}: {e}")
        return {"error": str(e)}

# --- 3. API KHỐI NGOẠI RIÊNG (tăng range 90 ngày) ---
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

# --- 4. API TIN TỨC (giữ nguyên) ---
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
    except: return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


