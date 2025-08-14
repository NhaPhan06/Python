import pandas as pd
import uuid
from datetime import datetime

# Đọc file Excel
df = pd.read_excel("hanh_chinh.xlsx", engine="openpyxl")

COUNTRY_ID = '8983c2f2-c57d-4a7e-b229-3b4677c3560d'
now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def make_sql(id_, name, type_, country_id, parent_id=None):
    parent_val = f"'{parent_id}'" if parent_id else "NULL"
    return f"INSERT INTO location_unit (id, name, type, country_id, parent_id, created_at, updated_at) VALUES ('{id_}', '{name}', '{type_}', '{country_id}', {parent_val}, '{now}', '{now}');"

province_map = {}
district_map = {}
sql_statements = []

for _, row in df.iterrows():
    province_name = row["Tỉnh Thành Phố"].replace("'", "''")
    district_name = row["Quận Huyện"].replace("'", "''")
    ward_name = row["Phường Xã"].replace("'", "''")

    # Tỉnh / Thành phố
    if province_name not in province_map:
        pid = str(uuid.uuid4())
        province_map[province_name] = pid
        sql_statements.append(make_sql(pid, province_name, "CITY", COUNTRY_ID))

    # Quận / Huyện
    district_key = (province_name, district_name)
    if district_key not in district_map:
        did = str(uuid.uuid4())
        district_map[district_key] = did
        sql_statements.append(make_sql(did, district_name, "DISTRICT", COUNTRY_ID, province_map[province_name]))

    # Phường / Xã
    wid = str(uuid.uuid4())
    sql_statements.append(make_sql(wid, ward_name, "WARD", COUNTRY_ID, district_map[district_key]))

# Xuất file SQL
with open("location_units.sql", "w", encoding="utf-8") as f:
    f.write("\n".join(sql_statements))

