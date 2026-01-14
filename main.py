from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import Vnstock
import pandas as pd
from datetime import datetime, timedelta
import requests

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
    return {"message": "Stock API with VNDirect Source is running!"}

# --- 1. API LẤY GIÁ (GIỮ NGUYÊN) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        
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

# --- 2. HÀM LẤY TIN TỪ VNDIRECT (JSON API - CỰC ỔN ĐỊNH) ---
def get_vndirect_news(symbol):
    try:
        # API công khai của VNDirect (trả về JSON, không cần parse HTML)
        url = "https://finfo-api.vndirect.com.vn/v4/news"
        params = {
            "symbol": symbol.upper(),
            "pageSize": 10,
            "page": 1
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and len(data['data']) > 0:
                news_list = []
                for item in data['data']:
                    # Tạo link tin tức (VNDirect News)
                    news_id = item.get('newsId')
                    # Format link: https://dautu.vndirect.com.vn/tin-tuc/tieu-de-slug-{newsId}
                    # Để đơn giản ta dẫn về trang news chung hoặc tạo link giả lập
                    article_link = f"https://dautu.vndirect.com.vn/tin-tuc/chi-tiet-{news_id}"
                    
                    news_list.append({
                        "title": item.get('newsTitle'),
                        "publishdate": item.get('newsDate', '').split('T')[0], # Lấy phần ngày YYYY-MM-DD
                        "link": article_link,
                        "source": "VNDirect"
                    })
                return news_list
        return []
    except Exception as e:
        print(f"VNDirect Error: {e}")
        return []

# --- 3. API TỔNG HỢP ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    print(f"Đang lấy tin cho {symbol}...")
    
    # Ưu tiên 1: TCBS (Vnstock)
    try:
        stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        df = stock.news()
        if df is not None and not df.empty:
            print("=> Lấy từ TCBS thành công")
            df.columns = [c.lower() for c in df.columns]
            return df.head(10).to_dict(orient='records')
    except:
        pass
        
    # Ưu tiên 2: VNDirect API (Dự phòng xịn)
    print("=> TCBS rỗng, chuyển sang VNDirect API...")
    vndirect_data = get_vndirect_news(symbol)
    
    if vndirect_data:
        print(f"=> Lấy được {len(vndirect_data)} tin từ VNDirect")
        return vndirect_data
        
    return []
