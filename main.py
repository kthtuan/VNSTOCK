from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import Vnstock
import pandas as pd
from datetime import datetime, timedelta
import requests
import re

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
    return {"message": "Stock API with News Crawler is running!"}

# --- 1. API LẤY GIÁ (GIỮ NGUYÊN) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        
        # Thử VCI trước
        try:
            stock = Vnstock().stock(symbol=symbol.upper(), source='VCI')
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        except:
            stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')

        if df is None or df.empty: return []

        df.columns = [col.lower() for col in df.columns]
        if 'time' in df.columns: df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        elif 'tradingdate' in df.columns: df['date'] = pd.to_datetime(df['tradingdate']).dt.strftime('%Y-%m-%d')
            
        if df['close'].iloc[-1] < 500:
             for c in ['open', 'high', 'low', 'close']: 
                 if c in df.columns: df[c] = df[c] * 1000

        return df[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records')
    except Exception as e:
        print(f"Stock Error: {e}")
        return []

# --- 2. HÀM CRAWLER TIN TỨC CAFEF (MỚI) ---
def crawl_cafef_news(symbol):
    """
    Hàm này tự động 'đọc' trang tin tức CafeF của mã cổ phiếu
    khi API chính thống bị lỗi hoặc rỗng.
    """
    try:
        url = f"https://s.cafef.vn/tin-tuc/{symbol}.chn"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return []

        # Dùng Pandas để tìm bảng tin tức (thường CafeF có cấu trúc list)
        # Hoặc dùng Regex đơn giản để lấy Tiêu đề và Link (nhẹ hơn cài thêm thư viện bs4)
        html = response.text
        
        # Regex tìm các bài tin trong thẻ <li> hoặc <div> class="news-item"
        # Cấu trúc thường thấy: title="Tiêu đề..." href="/tin-tuc/..."
        # Lấy 10 tin đầu tiên
        pattern = r'<a[^>]*href="([^"]+)"[^>]*title="([^"]+)"[^>]*>(.*?)</a>'
        matches = re.findall(pattern, html)
        
        news_list = []
        seen_titles = set()
        
        for link, title, text in matches:
            if title in seen_titles: continue
            if len(title) < 10: continue # Bỏ qua tin rác
            
            seen_titles.add(title)
            
            # Xử lý link (nếu link là tương đối)
            full_link = link if link.startswith("http") else f"https://cafef.vn{link}"
            
            # Xử lý ngày (CafeF thường có ngày trong thẻ span bên cạnh, nhưng để đơn giản ta lấy ngày hiện tại hoặc để trống)
            # Hoặc regex tìm ngày: <span class="time">...</span>
            
            news_list.append({
                "title": title.strip(),
                "link": full_link,
                "publishdate": datetime.now().strftime('%Y-%m-%d'), # Tạm thời để ngày hôm nay
                "source": "CafeF (Crawler)"
            })
            
            if len(news_list) >= 10: break
            
        return news_list
    except Exception as e:
        print(f"Crawler Error: {e}")
        return []

# --- 3. API LẤY TIN TỨC (THÔNG MINH) ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        print(f"Đang lấy tin cho {symbol}...")
        
        # CÁCH 1: Thử nguồn TCBS (Chính thống)
        try:
            stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
            df = stock.news()
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                return df.head(10).to_dict(orient='records')
        except:
            pass
            
        # CÁCH 2: Nếu TCBS rỗng -> Dùng Crawler CafeF (Dự phòng hạng nặng)
        print("TCBS rỗng, kích hoạt Crawler CafeF...")
        crawler_data = crawl_cafef_news(symbol)
        
        if crawler_data:
            return crawler_data
            
        return []
    except Exception as e:
        print(f"News API Error: {e}")
        return []
