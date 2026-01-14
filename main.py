from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Import thư viện
from vnstock import Vnstock, Quote 
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse

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
    return {"message": "Stock API is running!"}

# --- HÀM THÔNG MINH: TỰ ĐỘNG ĐỔI NGUỒN (Đã thêm SSI) ---
def get_stock_data_safe(symbol: str, start_date: str, end_date: str):
    # 1. Thử TCBS (Ưu tiên số 1: Dữ liệu xịn nhất)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='TCBS')
        df = quote.history()
        if df is not None and not df.empty:
            print(f"Lấy từ TCBS thành công: {symbol}")
            return df
    except:
        pass # Lặng lẽ bỏ qua để thử nguồn khác

    # 2. Thử SSI (Ưu tiên số 2: Có khối ngoại, ít bị chặn hơn TCBS)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='SSI')
        df = quote.history()
        if df is not None and not df.empty:
            print(f"Lấy từ SSI thành công: {symbol}")
            return df
    except:
        pass

    # 3. Thử DNSE
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='DNSE')
        df = quote.history()
        if df is not None and not df.empty:
            print(f"Lấy từ DNSE thành công: {symbol}")
            return df
    except:
        pass

    # 4. Đường cùng: VCI (Chỉ có giá, không có khối ngoại)
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty:
            print(f"Fallback về VCI (Không có khối ngoại): {symbol}")
            return df
    except Exception as e:
        print(f"Thất bại toàn tập với {symbol}: {e}")
    
    return None

# --- API LẤY GIÁ ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)
        if df is None or df.empty: return []

        df.columns = [col.lower() for col in df.columns]
        
        # Xử lý ngày tháng
        if 'time' in df.columns: df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        elif 'tradingdate' in df.columns: df['date'] = pd.to_datetime(df['tradingdate']).dt.strftime('%Y-%m-%d')
            
        # Fix lỗi giá x1000
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
             for c in ['open', 'high', 'low', 'close']: 
                 if c in df.columns: df[c] = df[c] * 1000

        # Trả về các cột cơ bản
        cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        return df[[c for c in cols if c in df.columns]].to_dict(orient='records')
    except Exception as e:
        print(f"Stock Error: {e}")
        return []

# --- API TIN TỨC (Google News) ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn)'
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=vi&gl=VN&ceid=VN:vi"
        feed = feedparser.parse(rss_url)
        
        news_list = []
        for entry in feed.entries[:10]:
            try:
                dt = entry.published_parsed
                date_str = f"{dt.tm_year}-{dt.tm_mon:02d}-{dt.tm_mday:02d}" if dt else ""
            except: date_str = ""
            
            news_list.append({
                "title": entry.title,
                "link": entry.link,
                "publishdate": date_str,
                "source": "Google News"
            })
        return news_list
    except: return []

# --- API KHỐI NGOẠI (Cập nhật logic lấy nhiều tên cột) ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)
        if df is None or df.empty: return []

        results = []
        for index, row in df.iterrows():
            # 1. Cố gắng lấy Mua/Bán (TCBS/SSI hay dùng tên khác nhau)
            buy = 0.0
            sell = 0.0
            
            # Các biến thể tên cột mua
            for col in ['foreign_buy', 'nn_mua', 'buy_foreign_qtty', 'buy_total_qtty']:
                if col in row and row[col]: buy = float(row[col]); break
            
            # Các biến thể tên cột bán
            for col in ['foreign_sell', 'nn_ban', 'sell_foreign_qtty', 'sell_total_qtty']:
                if col in row and row[col]: sell = float(row[col]); break
            
            # Tính ròng
            net = buy - sell
            
            # Nếu vẫn bằng 0, thử tìm cột Net trực tiếp
            if net == 0:
                for col in ['khoi_luong_rong', 'net_value', 'net_foreign_vol']:
                    if col in row and row[col]: net = float(row[col]); break

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
