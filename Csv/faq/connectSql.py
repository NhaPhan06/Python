import psycopg2
from faq import faq_import

location_units = faq_import("Affiliate.csv", "47f6ff8f-6253-4ae0-bd01-c40ff4eafea7")
print(len(location_units))

conn = psycopg2.connect(
    host="14.225.218.96",
    port=5432,
    database="NFT",
    user="postgres",
    password="Admin@123"
)
cur = conn.cursor()

for unit in location_units:
    print(unit)
    cur.execute("""
        INSERT INTO faq (id, question, answer, "typeFaqId")
        VALUES (%s, %s, %s, %s)
    """, (unit["id"], unit["question"], unit["answer"], unit["typeFaqId"])
    )
conn.commit()
cur.close()
conn.close()
