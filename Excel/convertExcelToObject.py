import pandas as pd
import uuid
from models import LocationUnit, LocationUnitType

COUNTRY_ID = '8983c2f2-c57d-4a7e-b229-3b4677c3560d'

def read_excel_to_location_units(file_path):
    df = pd.read_excel(file_path, engine="openpyxl")
    province_map = {}
    district_map = {}
    location_units = []

    for _, row in df.iterrows():
        province_name = row["Tỉnh Thành Phố"].replace("'", "''")
        district_name = row["Quận Huyện"].replace("'", "''")
        ward_name = row["Phường Xã"].replace("'", "''")

        # Province
        if province_name not in province_map:
            pid = str(uuid.uuid4())
            province_map[province_name] = pid
            location_units.append(LocationUnit(
                id=pid,
                name=province_name,
                type=LocationUnitType.CITY,
                country_id=COUNTRY_ID,
            ))

        # District
        district_key = (province_name, district_name)
        if district_key not in district_map:
            did = str(uuid.uuid4())
            district_map[district_key] = did
            location_units.append(LocationUnit(
                id=did,
                name=district_name,
                type=LocationUnitType.DISTRICT,
                country_id=COUNTRY_ID,
                parent_id=province_map[province_name],
            ))

        # Ward
        wid = str(uuid.uuid4())
        location_units.append(LocationUnit(
            id=wid,
            name=ward_name,
            type=LocationUnitType.WARD,
            country_id=COUNTRY_ID,
            parent_id=district_map[district_key],
        ))

    return location_units

# Cho phép chạy thử riêng
if __name__ == "__main__":
    units = read_excel_to_location_units("hanh_chinh.xlsx")
    for u in units[:5]:
        print(u.name, u.type.value, u.parent_id)
