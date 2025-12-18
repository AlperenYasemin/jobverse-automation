import os
import requests
import pandas as pd
import pymongo
import certifi
import re
from datetime import datetime

# --- AYARLAR ---
# GitHub'daki kasadan şifreleri alıyoruz
MONGO_URI = os.environ.get("MONGO_URI")
RAPID_API_KEY = os.environ.get("RAPID_API_KEY")


# --- ADIM 1: VERİ ÇEKME (API) ---
# Pages sayısını 10'a çıkardık (Haftalık ~150 ilan)
def fetch_job_data(query="Developer", pages=10):
    print(f"⏳ '{query}' için API'den {pages} sayfa veri çekiliyor...")
    all_jobs = []

    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    for page in range(1, pages + 1):
        querystring = {"query": query, "page": str(page), "num_pages": "1"}
        try:
            response = requests.get(url, headers=headers, params=querystring)
            data = response.json().get('data', [])
            all_jobs.extend(data)
            print(f"   -> Sayfa {page} çekildi. (Toplam havuz: {len(all_jobs)} ilan)")
        except Exception as e:
            print(f"❌ Hata (Sayfa {page}): {e}")

    return pd.DataFrame(all_jobs)


# --- ADIM 2: ANALİZ VE KAYDETME ---
def run_pipeline():
    # A) Veritabanına Bağlan
    try:
        client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
        db = client["JobverseDB"]

        # İKİ AYRI TABLO TANIMLIYORUZ:
        analysis_col = db["daily_insights"]  # Analiz Sonuçları
        raw_col = db["raw_jobs_list"]  # Ham İlanlar (Ana Tablo)

        print("✅ Veritabanına bağlanıldı.")
    except Exception as e:
        print("❌ DB Bağlantı Hatası:", e)
        return

    # --- TEMİZLİK (HER HAFTA YENİ SAYFA) ---
    # Eski analizleri sil
    analysis_col.delete_many({})
    # Eski ham ilanları sil (Veriler güncel kalsın dediğin için)
    raw_col.delete_many({})
    print("🧹 Eski veriler temizlendi.")

    # B) Veriyi Getir (10 Sayfa)
    df = fetch_job_data(query="Developer", pages=10)

    if df.empty:
        print("⚠️ Veri gelmedi, işlem iptal.")
        return

    # --- C) HAM VERİYİ KAYDET (RAW DATA) ---
    print(f"💾 {len(df)} adet ham ilan 'raw_jobs_list' tablosuna kaydediliyor...")
    # DataFrame'i sözlüğe çevirip toplu yüklüyoruz
    raw_col.insert_many(df.to_dict(orient='records'))
    print("✅ Ham veriler yüklendi.")

    # --- D) ANALİZ SÜRECİ ---
    print("📊 Analizler hesaplanıyor...")

    # Ön İşlemler
    df['job_description'] = df['job_description'].fillna('').astype(str).str.lower()

    if 'job_min_salary' in df.columns and 'job_max_salary' in df.columns:
        df['avg_salary'] = (df['job_min_salary'] + df['job_max_salary']) / 2
    else:
        df['avg_salary'] = None

    if 'job_posted_at_datetime_utc' in df.columns:
        df['date_obj'] = pd.to_datetime(df['job_posted_at_datetime_utc'], errors='coerce')
        df['day_name'] = df['date_obj'].dt.day_name()

    # Rapor Hazırlığı
    daily_report = {
        "report_date": datetime.now(),
        "query": "Developer",
        "total_jobs_analyzed": len(df)
    }

    # 1. En Popüler Unvanlar
    if 'job_title' in df.columns:
        daily_report["1_top_titles"] = df['job_title'].value_counts().head(10).reset_index().rename(
            columns={'index': 'job_title', 'job_title': 'count'}).to_dict(orient='records')

    # 2. Şehirler
    if 'job_city' in df.columns:
        daily_report["2_top_cities"] = df['job_city'].value_counts().head(10).reset_index().rename(
            columns={'index': 'city', 'job_city': 'count'}).to_dict(orient='records')

    # 3. Remote Durumu
    if 'job_is_remote' in df.columns:
        daily_report["3_remote_stats"] = df['job_is_remote'].value_counts().reset_index().rename(
            columns={'index': 'is_remote', 'job_is_remote': 'count'}).to_dict(orient='records')

    # 4. İşverenler
    if 'employer_name' in df.columns:
        daily_report["4_top_employers"] = df['employer_name'].value_counts().head(10).reset_index().rename(
            columns={'index': 'employer', 'employer_name': 'count'}).to_dict(orient='records')

    # 5. Maaş Analizi
    salary_df = df.dropna(subset=['avg_salary'])
    if not salary_df.empty:
        daily_report["5_salary_stats"] = {
            "min_avg": salary_df['avg_salary'].min(),
            "max_avg": salary_df['avg_salary'].max(),
            "mean_avg": salary_df['avg_salary'].mean(),
            "sample_size": len(salary_df)
        }
    else:
        daily_report["5_salary_stats"] = "Yeterli maaş verisi yok"

    # 6. Yayıncılar
    if 'job_publisher' in df.columns:
        daily_report["6_publishers"] = df['job_publisher'].value_counts().head(10).reset_index().rename(
            columns={'index': 'publisher', 'job_publisher': 'count'}).to_dict(orient='records')

    # 7. Yetenekler
    keywords = ['python', 'sql', 'java', 'react', 'aws', 'docker', 'kubernetes', 'c#', 'javascript', 'linux',
                'typescript', 'go']
    skill_counts = {kw: int(df['job_description'].apply(lambda x: kw in x).sum()) for kw in keywords}
    daily_report["7_top_skills"] = skill_counts

    # 8. Eyaletler
    if 'job_state' in df.columns:
        daily_report["8_top_states"] = df['job_state'].value_counts().head(10).reset_index().rename(
            columns={'index': 'state', 'job_state': 'count'}).to_dict(orient='records')

    # 9. Eğitim
    edu_keys = {'bachelor': ['bachelor', 'bs degree'], 'master': ['master', 'ms degree'], 'phd': ['phd', 'doctorate']}
    edu_res = {'bachelor': 0, 'master': 0, 'phd': 0}
    for desc in df['job_description']:
        for level, keys in edu_keys.items():
            if any(k in desc for k in keys):
                edu_res[level] += 1
    daily_report["9_education_levels"] = edu_res

    # 10. Haftanın Günleri
    if 'day_name' in df.columns:
        daily_report["10_posting_days"] = df['day_name'].value_counts().reset_index().rename(
            columns={'index': 'day', 'day_name': 'count'}).to_dict(orient='records')

    # 11. Deneyim
    def extract_years(text):
        match = re.search(r'(\d+)\+?\s*-?\s*(\d*)?\s*years?', text)
        if match: return int(match.group(1))
        return None

    df['exp_years'] = df['job_description'].apply(extract_years)
    try:
        bins = [0, 2, 5, 8, 50]
        labels = ['Junior (0-2)', 'Mid (3-5)', 'Senior (5-8)', 'Lead (8+)']
        daily_report["11_experience_levels"] = pd.cut(df['exp_years'], bins=bins,
                                                      labels=labels).value_counts().reset_index().rename(
            columns={'index': 'level', 'exp_years': 'count'}).to_dict(orient='records')
    except:
        daily_report["11_experience_levels"] = []

    # 12. Soft Skills
    soft_skills = ['communication', 'leadership', 'teamwork', 'english', 'problem solving']
    daily_report["12_soft_skills"] = {sk: int(df['job_description'].str.contains(sk).sum()) for sk in soft_skills}

    # 13. Skill / Maaş
    if not salary_df.empty:
        skill_roi = []
        for tech in ['python', 'java', 'react', 'aws']:
            mask = salary_df['job_description'].str.contains(tech)
            if mask.any():
                avg = salary_df[mask]['avg_salary'].mean()
                skill_roi.append({'skill': tech, 'avg_salary': round(avg, 2)})
        daily_report["13_skill_salary_roi"] = skill_roi
    else:
        daily_report["13_skill_salary_roi"] = "Yeterli maaş verisi yok"

    # 14. İstihdam Türü
    if 'job_employment_type' in df.columns:
        daily_report["14_employment_types"] = df['job_employment_type'].value_counts().reset_index().rename(
            columns={'index': 'type', 'job_employment_type': 'count'}).to_dict(orient='records')

    # --- E) ANALİZİ KAYDET ---
    analysis_col.insert_one(daily_report)
    print("-" * 40)
    print("✅ İŞLEM BAŞARILI!")
    print(f"   -> {len(df)} ham ilan 'raw_jobs_list' tablosuna eklendi.")
    print("   -> Tüm analizler 'daily_insights' tablosuna eklendi.")
    print("-" * 40)


if __name__ == "__main__":
    run_pipeline()
