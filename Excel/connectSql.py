import psycopg2
from convertExcelToObject import read_excel_to_location_units

location_units = read_excel_to_location_units("hanh_chinh.xlsx")

conn = psycopg2.connect(
    host="14.225.218.96",
    port=5432,
    database="NFT",
    user="postgres",
    password="Admin@123"
)
cur = conn.cursor()

for unit in location_units:
    cur.execute("""
        INSERT INTO location_unit (id, name, type, country_id, parent_id)
        VALUES (%s, %s, %s, %s, %s)
    """, (unit.id, unit.name, unit.type.value, unit.country_id, unit.parent_id))

conn.commit()
cur.close()
conn.close()

print(f"✅ Đã insert {len(location_units)} bản ghi")
