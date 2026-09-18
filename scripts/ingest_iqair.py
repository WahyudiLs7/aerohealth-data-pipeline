import requests
import os
import pandas as pd
from datetime import datetime
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

def fetch_iqair_data():
    API_KEY = os.environ.get('IQAIR_API_KEY')

    url = f"http://api.airvisual.com/v2/city?city=Pekanbaru&state=Riau&country=Indonesia&key={API_KEY}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        pollution = data['data']['current']['pollution']
        weather = data['data']['current']['weather']

        record = {
            "tanggal_ekstraksi": datetime.now().strftime('%Y-%m-%d'),
            "waktu_data": pollution['ts'],
            "lokasi": data['data']['city'],
            "aqi_us": pollution['aqius'],
            "polutan_utama": pollution['mainus'],
            "suhu_celcius": weather['tp'],
            "kelembapan_persen": weather['hu'],
            "kecepatan_angin_ms": weather['ws']
        }

        df = pd.DataFrame([record])

        file_name = f"iqair_pekanbaru_{datetime.now().strftime('%Y%m%d')}.csv"
        file_path = os.path.join(os.path.dirname(__file__), '..', 'data_lake', file_name)
        df.to_csv(file_path, index=False)

        print(f"✅ Sukses! Data IQAir tersimpan di {file_path}")
        print(df)

        upload_to_minio(file_path, f"iqair/{file_name}")

    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menarik API IQAir: {e}")

if __name__ == "__main__":
    fetch_iqair_data()