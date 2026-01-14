from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Import cả 2 class để dùng linh hoạt
from vnstock import Vnstock, Quote 
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse

app = FastAPI()

# Cấu hình CORS (Cho phép Frontend React gọi API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. ROOT API (GIỮ LẠI ĐỂ TEST SERVER) ---
@app.get("/")
def home():
    return {"message": "Stock API (Google News Gateway) is running!"}

# --- HÀM HỖ TRỢ: LẤY DỮ LIỆU AN TOÀN (Auto Switch Source) ---
# Hàm này giúp tự động đổi nguồn TCBS -> DNSE -> VCI nếu bị lỗi
def get_stock_data_safe(symbol: str, start_date: str, end_date: str):
    # Ưu tiên 1: TCBS (Dữ liệu xịn, có khối ngoại)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='TCBS')
        df = quote.history()
        if df is not None and not df.empty:
            return df
    except:
        print(f"TCBS lỗi với {symbol}, thử DNSE...")

    # Ưu tiên 2: DNSE (Ít bị chặn IP)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='DNSE')
        df = quote.history()
        if df is not None and not df.empty:
            return df
    except:
        print(f"DNSE lỗi với {symbol}, thử VCI...")

    # Ưu tiên 3: VCI (Cơ bản nhất)
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty:
            return df
    except Exception as e:
        print(f"Thất bại toàn tập với {symbol}: {e}")
    
    return None

# --- 2. API LẤY GIÁ CỔ PHIẾU ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d') # Lấy 3 năm
        
        # Gọi hàm an toàn
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)

        if df is None or df.empty: return []

        # Chuẩn hóa tên cột
        df.columns = [col.lower() for col in df.columns]
        
        # Xử lý cột ngày tháng
        if 'time' in df.columns: 
            df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        elif 'tradingdate' in df.columns: 
            df['date'] = pd.to_datetime(df['tradingdate']).dt.strftime('%Y-%m-%d')
            
        # Xử lý lỗi đơn vị giá (một số nguồn trả về 40 thay vì 40000)
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
             for c in ['open', 'high', 'low', 'close']: 
                 if c in df.columns: df[c] = df[c] * 1000

        # Chỉ trả về các cột cần thiết
        return df[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records')
    except Exception as e:
        print(f"Stock Error: {e}")
        return []

# --- 3. API LẤY TIN TỨC (GOOGLE NEWS) ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn OR site:tinnhanhchungkhoan.vn)'
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=vi&gl=VN&ceid=VN:vi"
        
        feed = feedparser.parse(rss_url)
        news_list = []
        
        for entry in feed.entries[:10]:
            published_parsed = entry.get("published_parsed")
            if published_parsed:
                date_str = f"{published_parsed.tm_year}-{published_parsed.tm_mon:02d}-{published_parsed.tm_mday:02d}"
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')

            news_list.append({
                "title": entry.title,
                "link": entry.link,
                "publishdate": date_str,
                "source": "Google News"
            })
        return news_list
    except Exception as e:
        print(f"News Error: {e}")
        return []

# --- 4. API LẤY KHỐI NGOẠI ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Gọi hàm an toàn để lấy dữ liệu
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)
        
        if df is None or df.empty: return []

        results = []
        for index, row in df.iterrows():
            # Logic map dữ liệu an toàn cho các nguồn khác nhau
            buy = float(row.get('foreign_buy', row.get('nn_mua', 0)) or 0)
            sell = float(row.get('foreign_sell', row.get('nn_ban', 0)) or 0)
            
            # Tính ròng
            net = buy - sell
            # Fallback nếu không có buy/sell mà chỉ có net
            if net == 0:
                net = float(row.get('khoi_luong_rong', row.get('net_value', 0)) or 0)

            results.append({
                "date": str(row.get('time', row.get('ngay', row.get('date', '')))),
                "buyVol": buy,
                "sellVol": sell,
                "netVolume": net
            })
            
        return results
    except Exception as e:
        print(f"Foreign Error {symbol}: {e}")
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
