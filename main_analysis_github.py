import os  # <--- Bunu en tepeye eklemeyi unutma
import requests
import pandas as pd
import pymongo
import certifi
import re
from datetime import datetime

# --- AYARLAR ---
# Artık şifreleri direkt buraya yazmıyoruz.
# Bilgisayara "Bu şifreleri gizli kasadan (Environment Variables) al" diyoruz.
MONGO_URI = os.environ.get("MONGO_URI")
RAPID_API_KEY = os.environ.get("RAPID_API_KEY")

# --- ADIM 1: VERİ ÇEKME ---
def fetch_job_data(query="Developer", pages=1):
    print(f"⏳ '{query}' için API'den veri çekiliyor...")
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
            print(f"   -> Sayfa {page} çekildi. (Toplam: {len(all_jobs)} ilan)")
        except Exception as e:
            print(f"❌ Hata (Sayfa {page}): {e}")

    return pd.DataFrame(all_jobs)


# --- ADIM 2: ANALİZ VE KAYDETME ---
def run_pipeline():
    # A) Veritabanına Bağlan
    try:
        client = pymongo.MongoClient(MONGO_URI, tlsCAFile=certifi.where())
        db = client["JobverseDB"]
        collection = db["daily_insights"]
        print("✅ Veritabanına bağlanıldı.")
    except Exception as e:
        print("❌ DB Bağlantı Hatası:", e)
        return

    # --- [YENİ EKLENEN KISIM] ---
    # Analize başlamadan önce eski verileri siliyoruz
    silinen = collection.delete_many({})
    print(f"🧹 Temizlik yapıldı: {silinen.deleted_count} eski rapor silindi.")
    # ----------------------------

    # B) Veriyi Getir
    df = fetch_job_data(query="Developer", pages=1)

    if df.empty:
        print("⚠️ Veri gelmedi, işlem iptal.")
        return

    print("📊 14 Analiz maddesi hesaplanıyor...")

    # --- ÖN İŞLEMLER ---
    df['job_description'] = df['job_description'].fillna('').astype(str).str.lower()

    if 'job_min_salary' in df.columns and 'job_max_salary' in df.columns:
        df['avg_salary'] = (df['job_min_salary'] + df['job_max_salary']) / 2
    else:
        df['avg_salary'] = None

    if 'job_posted_at_datetime_utc' in df.columns:
        df['date_obj'] = pd.to_datetime(df['job_posted_at_datetime_utc'], errors='coerce')
        df['day_name'] = df['date_obj'].dt.day_name()

    # --- RAPOR HAZIRLIĞI ---
    daily_report = {
        "report_date": datetime.now(),
        "query": "Developer",
        "total_jobs": len(df)
    }

    # 1. En Popüler Unvanlar
    if 'job_title' in df.columns:
        top_titles = df['job_title'].value_counts().head(10).reset_index()
        top_titles.columns = ['job_title', 'count']
        daily_report["1_top_titles"] = top_titles.to_dict(orient='records')

    # 2. Şehirler
    if 'job_city' in df.columns:
        top_cities = df['job_city'].value_counts().head(10).reset_index()
        top_cities.columns = ['city', 'count']
        daily_report["2_top_cities"] = top_cities.to_dict(orient='records')

    # 3. Remote Durumu
    if 'job_is_remote' in df.columns:
        remote = df['job_is_remote'].value_counts().reset_index()
        remote.columns = ['is_remote', 'count']
        daily_report["3_remote_stats"] = remote.to_dict(orient='records')

    # 4. İşverenler
    if 'employer_name' in df.columns:
        emps = df['employer_name'].value_counts().head(10).reset_index()
        emps.columns = ['employer', 'count']
        daily_report["4_top_employers"] = emps.to_dict(orient='records')

    # 5. Maaş Analizi
    salary_df = df.dropna(subset=['avg_salary'])
    if not salary_df.empty:
        salary_stats = {
            "min_avg": salary_df['avg_salary'].min(),
            "max_avg": salary_df['avg_salary'].max(),
            "mean_avg": salary_df['avg_salary'].mean(),
            "sample_size": len(salary_df)
        }
        daily_report["5_salary_stats"] = salary_stats
    else:
        daily_report["5_salary_stats"] = "Yeterli maaş verisi yok"

    # 6. Yayıncılar
    if 'job_publisher' in df.columns:
        pubs = df['job_publisher'].value_counts().head(10).reset_index()
        pubs.columns = ['publisher', 'count']
        daily_report["6_publishers"] = pubs.to_dict(orient='records')

    # 7. Yetenekler
    keywords = ['python', 'sql', 'java', 'react', 'aws', 'docker', 'kubernetes', 'c#', 'javascript', 'linux',
                'typescript', 'go']
    skill_counts = {}
    for kw in keywords:
        count = df['job_description'].apply(lambda x: kw in x).sum()
        skill_counts[kw] = int(count)
    daily_report["7_top_skills"] = skill_counts

    # 8. Eyaletler
    if 'job_state' in df.columns:
        states = df['job_state'].value_counts().head(10).reset_index()
        states.columns = ['state', 'count']
        daily_report["8_top_states"] = states.to_dict(orient='records')

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
        days = df['day_name'].value_counts().reset_index()
        days.columns = ['day', 'count']
        daily_report["10_posting_days"] = days.to_dict(orient='records')

    # 11. Deneyim
    def extract_years(text):
        match = re.search(r'(\d+)\+?\s*-?\s*(\d*)?\s*years?', text)
        if match: return int(match.group(1))
        return None

    df['exp_years'] = df['job_description'].apply(extract_years)
    try:
        bins = [0, 2, 5, 8, 50]
        labels = ['Junior (0-2)', 'Mid (3-5)', 'Senior (5-8)', 'Lead (8+)']
        exp_dist = pd.cut(df['exp_years'], bins=bins, labels=labels).value_counts().reset_index()
        exp_dist.columns = ['level', 'count']
        daily_report["11_experience_levels"] = exp_dist.to_dict(orient='records')
    except:
        daily_report["11_experience_levels"] = []

    # 12. Soft Skills
    soft_skills = ['communication', 'leadership', 'teamwork', 'english', 'problem solving']
    soft_res = {}
    for sk in soft_skills:
        soft_res[sk] = int(df['job_description'].str.contains(sk).sum())
    daily_report["12_soft_skills"] = soft_res

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
        e_types = df['job_employment_type'].value_counts().reset_index()
        e_types.columns = ['type', 'count']
        daily_report["14_employment_types"] = e_types.to_dict(orient='records')

    # --- ADIM 3: MONGODB'YE YÜKLEME ---
    collection.insert_one(daily_report)
    print("-" * 40)
    print("✅ İŞLEM TAMAM! Eski veriler silindi, yeni analiz yüklendi.")
    print("-" * 40)


if __name__ == "__main__":
    run_pipeline()