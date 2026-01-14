from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# Import cả 2 class để dùng linh hoạt
from vnstock import Vnstock, Quote 
import pandas as pd
from datetime import datetime, timedelta
import feedparser
import urllib.parse
import numpy as np # Thêm numpy để xử lý số liệu an toàn hơn

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
    return {"message": "Stock API is running (Shark Analysis + Smart Fallback)"}

# --- 1. HÀM HELPER: LẤY DỮ LIỆU AN TOÀN (Thử nhiều nguồn) ---
def get_stock_data_safe(symbol: str, start_date: str, end_date: str):
    # Ưu tiên 1: TCBS (Dữ liệu đầy đủ nhất)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='TCBS')
        df = quote.history()
        if df is not None and not df.empty: return df
    except: pass

    # Ưu tiên 2: SSI (Thường có khối ngoại, ít chặn hơn TCBS)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='SSI')
        df = quote.history()
        if df is not None and not df.empty: return df
    except: pass

    # Ưu tiên 3: DNSE (Dự phòng)
    try:
        quote = Quote(symbol=symbol, start=start_date, end=end_date, source='DNSE')
        df = quote.history()
        if df is not None and not df.empty: return df
    except: pass

    # Ưu tiên 4: VCI (Đường cùng - Chỉ có Giá, không có Khối ngoại)
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        if df is not None and not df.empty: return df
    except Exception as e:
        print(f"Lỗi lấy dữ liệu {symbol}: {e}")
    
    return None

# --- 2. API STOCK (TÍCH HỢP PHÂN TÍCH CÁ MẬP VSA) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # Gọi hàm lấy dữ liệu an toàn
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)

        if df is None or df.empty: return []

        # Chuẩn hóa tên cột
        df.columns = [col.lower() for col in df.columns]
        
        # Xử lý ngày tháng
        if 'time' in df.columns: df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        elif 'tradingdate' in df.columns: df['date'] = pd.to_datetime(df['tradingdate']).dt.strftime('%Y-%m-%d')
            
        # Fix lỗi đơn vị giá (nếu có)
        if 'close' in df.columns and df['close'].iloc[-1] < 500:
             for c in ['open', 'high', 'low', 'close']: 
                 if c in df.columns: df[c] = df[c] * 1000

        # === PHÂN TÍCH CÁ MẬP (VSA LOGIC) ===
        # Tính MA20 của Volume
        df['ma20_vol'] = df['volume'].rolling(window=20).mean()
        
        # Lấy dữ liệu mới nhất
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2] if len(df) > 1 else last_row
        
        vol_current = last_row['volume']
        vol_avg = last_row.get('ma20_vol', vol_current) # Nếu không có MA20 thì lấy vol hiện tại
        
        # Xử lý chia cho 0
        if pd.isna(vol_avg) or vol_avg == 0: vol_avg = 1
            
        price_change_pct = (last_row['close'] - prev_row['close']) / prev_row['close'] * 100
        vol_ratio = vol_current / vol_avg

        # Logic VSA đơn giản hóa (Gom hàng/Xả hàng)
        shark_action = "Lưỡng lự"
        shark_color = "neutral" # neutral, buy, sell

        if vol_ratio > 1.2: # Vol nổ (Lớn hơn 1.2 lần trung bình)
            if price_change_pct > 1.0:
                shark_action = "Gom hàng mạnh"
                shark_color = "buy"
            elif price_change_pct < -1.0:
                shark_action = "Xả hàng mạnh"
                shark_color = "sell"
            else:
                shark_action = "Biến động mạnh"
                shark_color = "warning"
        else: # Vol thấp
            if price_change_pct > 2: shark_action = "Kéo giá (Tiết cung)"
            elif price_change_pct < -2: shark_action = "Đè giá (Cạn vol)"
            else: shark_action = "Tích lũy"

        # === TRẢ VỀ KẾT QUẢ ===
        # Quan trọng: Frontend cần cập nhật để đọc được cấu trúc shark_analysis này
        return {
            "data": df[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records'),
            "shark_analysis": {
                "action": shark_action,
                "color": shark_color,
                "vol_ratio": round(vol_ratio, 2)
            }
        }

    except Exception as e:
        print(f"Stock Error: {e}")
        return []

# --- 3. API KHỐI NGOẠI (THỬ NHIỀU NGUỒN & MAP CỘT) ---
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Gọi hàm lấy dữ liệu an toàn
        df = get_stock_data_safe(symbol.upper(), start_date, end_date)
        
        if df is None or df.empty: return []

        results = []
        for index, row in df.iterrows():
            buy = 0.0
            sell = 0.0
            
            # Map tên cột từ nhiều nguồn khác nhau (TCBS/SSI/DNSE đều đặt tên khác nhau)
            # 1. Tìm cột Mua
            for col in ['foreign_buy', 'nn_mua', 'buy_foreign_qtty', 'buy_total_qtty']:
                if col in row and pd.notna(row[col]): 
                    buy = float(row[col])
                    break
            
            # 2. Tìm cột Bán
            for col in ['foreign_sell', 'nn_ban', 'sell_foreign_qtty', 'sell_total_qtty']:
                if col in row and pd.notna(row[col]): 
                    sell = float(row[col])
                    break
            
            net = buy - sell
            
            # 3. Nếu Net = 0, thử tìm cột Net trực tiếp (Fallback)
            if net == 0:
                 for col in ['khoi_luong_rong', 'net_value', 'net_foreign_vol']:
                    if col in row and pd.notna(row[col]): 
                        net = float(row[col])
                        break

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

# --- 4. API TIN TỨC (GIỮ NGUYÊN) ---
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
