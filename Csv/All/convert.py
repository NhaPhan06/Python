import csv
import uuid



def location_units(file_path, country_id, type_unit):
    units = []

    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            type = row["type"].strip()

            # nếu state chưa có -> thêm state
            if type == type_unit:
                units.append({
                    "id": row["id"].strip(),
                    "name": row["name"].strip(),
                    "type": row["type"].strip(),
                    "country_id": country_id,
                    "parent_id": None
                })
                
            else:
                units.append({
                    "id": str(uuid.uuid4()),
                    "name": row["name"].strip(),
                    "type": row["type"].strip(),
                    "country_id": country_id,
                    "parent_id": row["parent_id"].strip(),
                })


            print(units)
    return units
