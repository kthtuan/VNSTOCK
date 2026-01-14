from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from vnstock import Vnstock
import pandas as pd
from vnstock import stock_historical_data, stock_quote
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
    return {"message": "Stock API (Google News Gateway) is running!"}

# --- 1. API LẤY GIÁ (GIỮ NGUYÊN) ---
@app.get("/api/stock/{symbol}")
def get_stock(symbol: str):
    try:
        # Cố gắng lấy data, chấp nhận rủi ro bị chặn IP ở phần này
        # Nếu bị chặn nốt thì phải dùng giải pháp khác, nhưng thường API giá mở hơn API tin
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

# --- 2. HÀM LẤY TIN QUA GOOGLE NEWS RSS (KHÔNG LO CHẶN IP) ---
def get_google_stock_news(symbol):
    try:
        # Tạo câu truy vấn: "Mã CK" site:cafef.vn OR site:vietstock.vn ...
        # Chỉ lấy tin từ các trang uy tín để tránh rác
        query = f'"{symbol}" AND (site:cafef.vn OR site:vietstock.vn OR site:tinnhanhchungkhoan.vn)'
        encoded_query = urllib.parse.quote(query)
        
        # URL RSS của Google News tiếng Việt
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=vi&gl=VN&ceid=VN:vi"
        
        # Đọc RSS
        feed = feedparser.parse(rss_url)
        
        news_list = []
        for entry in feed.entries[:10]: # Lấy 10 tin đầu
            # Xử lý ngày tháng (Google trả về format phức tạp, ta lấy đơn giản)
            published_parsed = entry.get("published_parsed")
            if published_parsed:
                date_str = f"{published_parsed.tm_year}-{published_parsed.tm_mon:02d}-{published_parsed.tm_mday:02d}"
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')

            news_list.append({
                "title": entry.title,
                "link": entry.link,
                "publishdate": date_str,
                "source": "Google News (Aggregated)"
            })
            
        return news_list
    except Exception as e:
        print(f"Google RSS Error: {e}")
        return []

# --- 3. API TỔNG HỢP ---
@app.get("/api/news/{symbol}")
def get_stock_news(symbol: str):
    print(f"Đang lấy tin cho {symbol} qua Google News...")
    
    # Chỉ dùng duy nhất Google News vì nó ổn định nhất trên Cloud nước ngoài
    news = get_google_stock_news(symbol)
    
    if news:
        print(f"=> Lấy được {len(news)} tin.")
        return news
    
    return []
@app.get("/api/stock/foreign/{symbol}")
def get_foreign_flow(symbol: str):
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        # 1. Dùng nguồn TCBS (Nguồn này trả về đủ cột nước ngoài)
        stock = Vnstock().stock(symbol=symbol.upper(), source='TCBS')
        df = stock.quote.history(start=start_date, end=end_date, interval='1D')
        
        if df is None or df.empty: return []

        results = []
        for index, row in df.iterrows():
            # TCBS trả về cột 'foreign_buy' và 'foreign_sell' (hoặc nn_mua/nn_ban)
            # Dùng .get() để lấy an toàn
            buy = float(row.get('foreign_buy', row.get('nn_mua', 0)) or 0)
            sell = float(row.get('foreign_sell', row.get('nn_ban', 0)) or 0)
            
            # Tính ròng
            net = buy - sell
            
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


