import csv
import uuid


COUNTRY_ID = "5f315fa0-a18c-4383-8c26-28985f9fe3c5"

def mexico_csv_to_location_units(file_path):
    states = {}
    units = []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state_name = row["State"].strip()
            municipality_name = row["Municipality"].strip()

            # nếu state chưa có -> thêm state
            if state_name not in states:
                state_id = str(uuid.uuid4())
                states[state_name] = state_id
                units.append({
                    "id": state_id,
                    "name": state_name,
                    "type": "state",
                    "country_id": COUNTRY_ID,
                    "parent_id": None
                })

            # thêm municipio (district)
            units.append({
                "id": str(uuid.uuid4()),
                "name": municipality_name,
                "type": "district",
                "country_id": COUNTRY_ID,
                "parent_id": states[state_name]
            })

    return units
