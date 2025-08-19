import psycopg2
from convert import location_units

location_units_1 = location_units("Singapore.csv", "5ecad5a8-81bf-44fb-90f1-36cbdd59b703", "district")
location_units_2 = location_units("Canada.csv", "5a614100-e51a-4cfa-84b8-ba203ebe3674", "state")
location_units_3 = location_units("China.csv", "53220cdc-360a-4219-985c-33d3e693584d", "state")
location_units_4 = location_units("HongKong.csv", "3b8de987-60d0-46d1-bc4b-c4bc829ec2ec", "district")
location_units_5 = location_units("India.csv", "6e4118e5-9728-4fbb-b216-8a5a75d677ac", "state")
location_units_6 = location_units("Malaysia.csv", "7f13095c-c5d9-4bcd-b37c-d3bab2c0edb4", "state")
location_units_7 = location_units("Thailand.csv", "fe3374c2-fce1-4a01-8a74-8420a25112fa", "state")
location_units_8 = location_units("Mexico.csv", "5f315fa0-a18c-4383-8c26-28985f9fe3c5", "state")
location_units_9 = location_units("US.csv", "53aa936f-cead-427c-93a1-952cfe3f9139", "state")

location_units = location_units_1 + location_units_2 + location_units_3 + location_units_4 + location_units_5 + location_units_6 + location_units_7 + location_units_8 + location_units_9
 

conn = psycopg2.connect(
    host="14.225.218.96",
    port=5432,
    database="NFT",
    user="postgres",
    password="Admin@123"
)
cur = conn.cursor()
i= 1
for unit in location_units:
    print(i)
    i+=1
    cur.execute("""
        INSERT INTO location_unit (id, name, type, country_id, parent_id)
        VALUES (%s, %s, %s, %s, %s)
    """, (unit["id"], unit["name"], unit["type"], unit["country_id"], None if unit["parent_id"] == "null" else unit["parent_id"]))

conn.commit()
cur.close()
conn.close()
