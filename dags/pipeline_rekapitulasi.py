import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# connection id di airflow ui
POSTGRES_CONN_ID = "postgres_db_conn"

# url api dataa
url = "https://api-panelhargav2.badanpangan.go.id/table-rekapitulasi/export?province_id=14&level_harga_id=1&period_date=01%2F08%2F2025%20-%2031%2F08%2F2025"

def extract_data(tmp_file="data_raw.xlsx"):
    responses = requests.get(url)
    if responses.status_code == 200:
        with open(tmp_file, "wb") as f:
            f.write(responses.content)
        print(f"File berhasil diunduh: {tmp_file}")
        return tmp_file
    else:
        raise Exception(f"Gagal mengunduh file, status code: {responses.status_code}")

def clean_data(xlsx_file, cleaned_file="data_cleaned.csv"):
    df = pd.read_excel(xlsx_file, engine="openpyxl", header=2)
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if "No" in df.columns:
        mask = df["No"].astype(str).str.contains("Rata|Maks|Min", case=False, na=False)
        df = df[~mask]
    df = df.dropna(how="all")
    if "Tanggal" in df.columns:
        df["Tanggal"] = pd.to_datetime(df["Tanggal"], errors="coerce").dt.date
    numeric_cols = df.columns.drop([c for c in ["No", "Tanggal"] if c in df.columns])
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce").astype("Int64")
    if "No" in df.columns:
        df["No"] = pd.to_numeric(df["No"], errors="coerce").astype("Int64")
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[()]", "", regex=True)
        .str.replace(r"[/]", "_", regex=True)
    )
    df = df.drop_duplicates().reset_index(drop=True).dropna(how="all")
    df.to_csv(cleaned_file, index=False, encoding="utf-8-sig")
    print(f"data final: {df.shape[0]} baris, {df.shape[1]} kolom")
    return df

def load_to_postgres(cleaned_file="data_cleaned.csv"):
    df = pd.read_csv(cleaned_file)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Buat table jika belum ada
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS rekapitulasi_harga (
        no INT PRIMARY KEY,
        tanggal DATE,
        {', '.join([f"{col} INT" for col in df.columns if col not in ['no','tanggal']])}
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    print("Table siap")

    # Insert data row by row (row['no'] sebagai primary key)
    for _, row in df.iterrows():
        columns = ', '.join(row.index)
        placeholders = ', '.join(['%s'] * len(row))
        sql = f"""
            INSERT INTO rekapitulasi_harga ({columns})
            VALUES ({placeholders})
            ON CONFLICT (no) DO NOTHING;
        """
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data berhasil dimasukkan ke Postgres: {len(df)} baris")

# DAG Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_data_rekapitulasi',
    default_args=default_args,
    description='Pipeline untuk ambil data rekapitulasi dari endpoint API panel harga badan pangan',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    op_args=["data_raw.xlsx"],
    dag=dag
)

t3 = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    op_args=["data_cleaned.csv"],
    dag=dag
)

t1 >> t2 >> t3
