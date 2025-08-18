import psycopg2
from usa import csv_to_location_units

location_units = csv_to_location_units("county.csv", "states.csv", "independent_cities.csv")

print(len(location_units))

# conn = psycopg2.connect(
#     host="14.225.218.96",
#     port=5432,
#     database="NFT",
#     user="postgres",
#     password="Admin@123"
# )
# cur = conn.cursor()

#for unit in location_units:
    #print(unit)
    # cur.execute("""
    #     INSERT INTO location_unit (id, name, type, country_id, parent_id)
    #     VALUES (%s, %s, %s, %s, %s)
    # """, (unit["id"], unit["name"], unit["type"], unit["country_id"], unit["parent_id"]))

# conn.commit()
# cur.close()
# conn.close()
