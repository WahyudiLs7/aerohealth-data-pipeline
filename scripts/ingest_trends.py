from pytrends.request import TrendReq
import pandas as pd
import os
from datetime import datetime
import time
from minio import Minio

def upload_to_minio(file_path, object_name, bucket_name="aerohealth-raw"):
    try:
        client = Minio(
            os.environ.get('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.environ.get('MINIO_ACCESS_KEY'),
            secret_key=os.environ.get('MINIO_SECRET_KEY'),
            secure=False
        )
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Sukses upload ke MinIO: {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Gagal upload ke MinIO (tidak menghentikan pipeline): {e}")

def fetch_google_trends():
    pytrends = TrendReq(hl='id-ID', tz=420)
    kw_list = ["sesak napas", "obat batuk", "gejala dbd"]

    try:
        print("Menarik data dari Google Trends...")
        pytrends.build_payload(kw_list, cat=0, timeframe='today 1-m', geo='ID-RI')
        df_trends = pytrends.interest_over_time()

        if not df_trends.empty:
            df_trends = df_trends.drop(columns=['isPartial'])
            df_trends = df_trends.reset_index()
            df_trends.rename(columns={'date': 'tanggal_pencarian'}, inplace=True)

            file_name = f"google_trends_riau_{datetime.now().strftime('%Y%m%d')}.csv"
            file_path = os.path.join(os.path.dirname(__file__), '..', 'data_lake', file_name)
            df_trends.to_csv(file_path, index=False)

            print(f"✅ Sukses! Data Google Trends tersimpan di {file_path}")
            print(df_trends.tail())

            upload_to_minio(file_path, f"google-trends/{file_name}")
        else:
            print("⚠️ Tidak ada data pencarian yang memadai di rentang waktu ini.")

    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menarik API Google Trends: {e}")

if __name__ == "__main__":
    fetch_google_trends()