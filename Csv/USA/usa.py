import csv
import uuid
from models import LocationUnitType

COUNTRY_ID = '53aa936f-cead-427c-93a1-952cfe3f9139'

def csv_to_location_units(county_file, state_file, independent_city_file):
    
    # Read counties
    units_county = []
    with open(county_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["type"].lower() == "county":
                unit = {
                    "id": str(uuid.uuid4()),
                    "name": row["name"].strip(),
                    "type": LocationUnitType.COUNTY.value,
                    "state_name": row["state_name"].strip(),
                    "county_fips": row["county_fips"].strip(),
                }
                units_county.append(unit)
    
    # Read cities - Fixed: use county_file for cities from the same file
    units_cities = []
    with open(county_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["type"].lower() == "city":
                unit = {
                    "id": str(uuid.uuid4()),
                    "name": row["name"].strip(),
                    "type": LocationUnitType.CITY.value,
                    "state_name": row["state_name"].strip(),
                    "county_fips": row["county_fips"].strip(),
                }
                units_cities.append(unit)
    
    # Read independent cities if the file is different and provided
    if independent_city_file and independent_city_file != county_file:
        with open(independent_city_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                unit = {
                    "id": str(uuid.uuid4()),
                    "name": row["City"].strip(),
                    "type": LocationUnitType.CITY.value,
                    "state_name": row["State"].strip(),
                }
                units_cities.append(unit)
    
    print(f"Counties: {len(units_county)}, Cities: {len(units_cities)}")

    # Read states
    states = []
    with open(state_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            unit = {
                "id": str(uuid.uuid4()) if row["id"] == "" else row["id"],
                "name": row["name"],
                "type": LocationUnitType.STATE.value,
                "country_id": COUNTRY_ID,
                "parent_id": None,
            }
            states.append(unit)
    
    location_units = []
    
    # Add states to location_units first
    location_units.extend(states)
    print(f"Total states added: {len(states)}")

    # ---- Map state -> county ----
    # Create index: state_name -> state
    state_index = {s["name"]: s for s in states}

    for county in units_county:
        state = state_index.get(county["state_name"])
        if state:
            unit = {
                "id": county["id"],
                "name": county["name"],
                "type": county["type"],
                "country_id": COUNTRY_ID,
                "state_name": county["state_name"],  # keep for debugging if needed
                "parent_id": state["id"],
            }
            location_units.append(unit)
        else:
            print(f"Warning: State '{county['state_name']}' not found for county '{county['name']}'")

    print(f"Total including counties: {len(location_units)}")

    # ---- Map county -> city ----
    # Create index: county_fips -> county
    county_index = {c["county_fips"]: c for c in units_county}

    for city in units_cities:
        county = county_index.get(city["county_fips"])  # Fixed typo: was "coun nnty"
        if county:
            unit = {
                "id": city["id"],
                "name": city["name"],
                "type": city["type"],
                "country_id": COUNTRY_ID,
                "state_name": city["state_name"],
                "parent_id": county["id"],
            }
            location_units.append(unit)
        else:
            print(f"Warning: County with FIPS '{city['county_fips']}' not found for city '{city['name']}'")

    print(f"Total including cities: {len(location_units)}")

    return location_units