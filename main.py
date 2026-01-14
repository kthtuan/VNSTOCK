from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import Vnstock
import pandas as pd
from datetime import datetime, timedelta
import os

app = FastAPI()

# Cấu hình CORS (Cho phép mọi nguồn truy cập)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "Stock API is running on Render!"}

@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        # Lấy dữ liệu 3 năm
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        
        print(f"Fetching data for: {symbol}")
        
        # Thử nguồn VCI trước
        try:
            stock = Vnstock().stock(symbol=symbol.upper(), source='VCI')
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        except:
            print("VCI failed, trying TCBS...")
            stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
            df = stock.quote.history(start=start_date, end=end_date, interval='1D')

        if df is None or df.empty:
            return []

        # Chuẩn hóa dữ liệu
        df.columns = [col.lower() for col in df.columns]
        
        # Xử lý ngày tháng
        if 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        elif 'tradingdate' in df.columns:
            df['date'] = pd.to_datetime(df['tradingdate']).dt.strftime('%Y-%m-%d')
            
        # Fix lỗi đơn vị nghìn đồng
        if df['close'].iloc[-1] < 500:
             cols = ['open', 'high', 'low', 'close']
             for c in cols: 
                 if c in df.columns: df[c] = df[c] * 1000

        result = df[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records')
        return result

    except Exception as e:
        print(f"Error: {e}")
        return []
# --- API LẤY TIN TỨC MỚI ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    try:
        # Dùng nguồn TCBS để lấy tin tức (VCI thường ít tin hơn)
        stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        df = stock.news()
        
        if df is not None and not df.empty:
            # Chuẩn hóa tên cột
            df.columns = [c.lower() for c in df.columns]
            
            # Chỉ lấy 10 tin mới nhất
            return df.head(10).to_dict(orient='records')
            
        return []
    except Exception as e:
        print(f"❌ Lỗi lấy tin tức: {e}")
        return []