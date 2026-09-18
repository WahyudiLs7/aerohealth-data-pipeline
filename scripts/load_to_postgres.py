import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def load_data():
    db_url = 'postgresql://admin:adminpassword@postgres_dwh:5432/aerohealth_dwh'
    engine = create_engine(db_url)
    
    tanggal_hari_ini = datetime.now().strftime('%Y%m%d')
    
    base_path = os.path.join(os.path.dirname(__file__), '..', 'data_lake')
    file_iqair = os.path.join(base_path, f"iqair_pekanbaru_{tanggal_hari_ini}.csv")
    file_trends = os.path.join(base_path, f"google_trends_riau_{tanggal_hari_ini}.csv")
    
    try:
        print("Memulai proses load ke PostgreSQL...")
        
        if os.path.exists(file_iqair):
            df_iqair = pd.read_csv(file_iqair)
            df_iqair.to_sql('raw_iqair', engine, if_exists='append', index=False)
            print(f"✅ Data IQAir berhasil dimuat ke tabel 'raw_iqair'.")
        else:
            print(f"⚠️ File {file_iqair} tidak ditemukan.")

        if os.path.exists(file_trends):
            df_trends = pd.read_csv(file_trends)
            df_trends.to_sql('raw_google_trends', engine, if_exists='append', index=False)
            print(f"✅ Data Google Trends berhasil dimuat ke tabel 'raw_google_trends'.")
        else:
            print(f"⚠️ File {file_trends} tidak ditemukan.")
            
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat memuat data: {e}")

if __name__ == "__main__":
    load_data()