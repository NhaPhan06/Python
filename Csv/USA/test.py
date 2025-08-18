import requests
import json
import time
import uuid
from typing import Dict, List, Any, Optional
import enum
from sqlalchemy import create_engine, Column, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
import psycopg2

Base = declarative_base()

class LocationUnitType(str, enum.Enum):
    STATE = "state"
    CITY = "city"
    COUNTY = "county"

class LocationUnit(Base):
    __tablename__ = "location_unit"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())
    deletedAt = Column(DateTime, nullable=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    country_id = Column(String, default="53aa936f-cead-427c-93a1-952cfe3f9139")
    parent_id = Column(String, ForeignKey("location_unit.id"), nullable=True)
    
    # Relationship
    children = relationship("LocationUnit", backref="parent", remote_side=[id])

class USLocationImporter:
    def __init__(self):
        # Database connection
        self.conn = psycopg2.connect(
            host="14.225.218.96",
            port=5432,
            database="NFT",
            user="postgres",
            password="Admin@123"
        )
        self.cursor = self.conn.cursor()
        
        # Country ID cố định
        self.country_id = "53aa936f-cead-427c-93a1-952cfe3f9139"
        
    def get_states_data(self) -> List[Dict]:
        """Danh sách các bang Mỹ"""
        return [
            {"code": "01", "name": "Alabama", "abbr": "AL"},
            {"code": "02", "name": "Alaska", "abbr": "AK"},
            {"code": "04", "name": "Arizona", "abbr": "AZ"},
            {"code": "05", "name": "Arkansas", "abbr": "AR"},
            {"code": "06", "name": "California", "abbr": "CA"},
            {"code": "08", "name": "Colorado", "abbr": "CO"},
            {"code": "09", "name": "Connecticut", "abbr": "CT"},
            {"code": "10", "name": "Delaware", "abbr": "DE"},
            {"code": "11", "name": "District of Columbia", "abbr": "DC"},
            {"code": "12", "name": "Florida", "abbr": "FL"},
            {"code": "13", "name": "Georgia", "abbr": "GA"},
            {"code": "15", "name": "Hawaii", "abbr": "HI"},
            {"code": "16", "name": "Idaho", "abbr": "ID"},
            {"code": "17", "name": "Illinois", "abbr": "IL"},
            {"code": "18", "name": "Indiana", "abbr": "IN"},
            {"code": "19", "name": "Iowa", "abbr": "IA"},
            {"code": "20", "name": "Kansas", "abbr": "KS"},
            {"code": "21", "name": "Kentucky", "abbr": "KY"},
            {"code": "22", "name": "Louisiana", "abbr": "LA"},
            {"code": "23", "name": "Maine", "abbr": "ME"},
            {"code": "24", "name": "Maryland", "abbr": "MD"},
            {"code": "25", "name": "Massachusetts", "abbr": "MA"},
            {"code": "26", "name": "Michigan", "abbr": "MI"},
            {"code": "27", "name": "Minnesota", "abbr": "MN"},
            {"code": "28", "name": "Mississippi", "abbr": "MS"},
            {"code": "29", "name": "Missouri", "abbr": "MO"},
            {"code": "30", "name": "Montana", "abbr": "MT"},
            {"code": "31", "name": "Nebraska", "abbr": "NE"},
            {"code": "32", "name": "Nevada", "abbr": "NV"},
            {"code": "33", "name": "New Hampshire", "abbr": "NH"},
            {"code": "34", "name": "New Jersey", "abbr": "NJ"},
            {"code": "35", "name": "New Mexico", "abbr": "NM"},
            {"code": "36", "name": "New York", "abbr": "NY"},
            {"code": "37", "name": "North Carolina", "abbr": "NC"},
            {"code": "38", "name": "North Dakota", "abbr": "ND"},
            {"code": "39", "name": "Ohio", "abbr": "OH"},
            {"code": "40", "name": "Oklahoma", "abbr": "OK"},
            {"code": "41", "name": "Oregon", "abbr": "OR"},
            {"code": "42", "name": "Pennsylvania", "abbr": "PA"},
            {"code": "44", "name": "Rhode Island", "abbr": "RI"},
            {"code": "45", "name": "South Carolina", "abbr": "SC"},
            {"code": "46", "name": "South Dakota", "abbr": "SD"},
            {"code": "47", "name": "Tennessee", "abbr": "TN"},
            {"code": "48", "name": "Texas", "abbr": "TX"},
            {"code": "49", "name": "Utah", "abbr": "UT"},
            {"code": "50", "name": "Vermont", "abbr": "VT"},
            {"code": "51", "name": "Virginia", "abbr": "VA"},
            {"code": "53", "name": "Washington", "abbr": "WA"},
            {"code": "54", "name": "West Virginia", "abbr": "WV"},
            {"code": "55", "name": "Wisconsin", "abbr": "WI"},
            {"code": "56", "name": "Wyoming", "abbr": "WY"}
        ]
    
    def get_counties_from_census(self, state_code: str) -> List[Dict]:
        """Lấy counties và independent cities từ Census API - tất cả đều lưu như COUNTY"""
        counties = []
        
        try:
            url = f"https://api.census.gov/data/2020/dec/pl?get=NAME&for=county:*&in=state:{state_code}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                for row in data[1:]:  # Bỏ header
                    full_name = row[0]
                    county_code = row[2]
                    
                    # Làm sạch tên - loại bỏ suffix
                    clean_name = full_name
                    if " County" in full_name:
                        clean_name = full_name.replace(" County", "")
                    elif " Parish" in full_name:
                        clean_name = full_name.replace(" Parish", "")
                    elif " Borough" in full_name:
                        clean_name = full_name.replace(" Borough", "")
                    elif " Census Area" in full_name:
                        clean_name = full_name.replace(" Census Area", "")
                    elif " city" in full_name.lower() and state_code == "51":  # Virginia independent cities
                        clean_name = full_name.replace(" city", "")
                    
                    # Tất cả đều lưu là COUNTY (bao gồm cả independent cities)
                    counties.append({
                        "code": county_code,
                        "name": clean_name,
                        "full_name": full_name,
                        "type": LocationUnitType.COUNTY  # Tất cả đều là COUNTY
                    })
            
        except Exception as e:
            print(f"Lỗi khi lấy dữ liệu cho bang {state_code}: {e}")
            
        return counties
    
    def get_cities_from_census(self, state_code: str) -> List[Dict]:
        """Lấy danh sách cities/towns từ Census API - tất cả đều lưu như CITY"""
        cities = []
        
        try:
            url = f"https://api.census.gov/data/2020/dec/pl?get=NAME&for=place:*&in=state:{state_code}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                for row in data[1:]:  # Bỏ header
                    city_name = row[0]
                    place_code = row[2]
                    
                    # Tất cả đều lưu là CITY
                    cities.append({
                        "code": place_code,
                        "name": city_name,
                        "type": LocationUnitType.CITY
                    })
                    
        except Exception as e:
            print(f"Lỗi khi lấy cities cho bang {state_code}: {e}")
            
        return cities
    
    def save_location_unit(self, name: str, type_value: str, parent_id: str = None) -> str:
        """Lưu location unit vào database"""
        location_id = str(uuid.uuid4())
        
        insert_query = """
        INSERT INTO location_unit (id, "createdAt", "updatedAt", name, type, country_id, parent_id)
        VALUES (%s, NOW(), NOW(), %s, %s, %s, %s)
        """
        
        self.cursor.execute(insert_query, (
            location_id,
            name,
            type_value,
            self.country_id,
            parent_id
        ))
        self.conn.commit()
        
        return location_id
    
    def import_all_data(self, limit_states: int = 50, limit_counties: int = 500 , limit_cities: int = 1000):
        """Import toàn bộ dữ liệu vào database với 3 cấp: STATE -> COUNTY -> CITY"""
        print("🚀 Bắt đầu import dữ liệu hành chính Mỹ (3 cấp: STATE -> COUNTY -> CITY)...")
        
        states_data = self.get_states_data()
        
        for i, state_data in enumerate(states_data[:limit_states], 1):
            print(f"\n📍 Xử lý bang {i}/{limit_states}: {state_data['name']}")
            
            # 1. Lưu STATE
            state_id = self.save_location_unit(
                name=state_data["name"],
                type_value=LocationUnitType.STATE.value,
                parent_id=None
            )
            print(f"  ✅ Đã lưu STATE: {state_data['name']}")
            
            # 2. Lấy và lưu COUNTIES (bao gồm independent cities)
            counties = self.get_counties_from_census(state_data["code"])
            
            for j, county in enumerate(counties[:limit_counties], 1):
                print(f"  📍 Xử lý county {j}/{limit_counties}: {county['name']}")
                
                # Lưu COUNTY
                county_id = self.save_location_unit(
                    name=county["name"],
                    type_value=LocationUnitType.COUNTY.value,
                    parent_id=state_id
                )
                print(f"    ✅ Đã lưu COUNTY: {county['name']}")
                
                # 3. Lấy và lưu CITIES cho county này
                cities = self.get_cities_from_census(state_data["code"])
                
                city_count = 0
                for city in cities[:limit_cities]:
                    if city_count >= limit_cities:
                        break
                    
                    city_id = self.save_location_unit(
                        name=city["name"],
                        type_value=LocationUnitType.CITY.value,
                        parent_id=county_id
                    )
                    city_count += 1
                
                if city_count > 0:
                    print(f"    ✅ Đã lưu {city_count} CITIES")
                
                time.sleep(0.1)  # Rate limiting
            
            time.sleep(0.2)
        
        print(f"\n🎉 Hoàn thành import dữ liệu!")
        self.print_statistics()
    
    def print_statistics(self):
        """In thống kê dữ liệu đã import"""
        print("\n📊 THỐNG KÊ DỮ LIỆU ĐÃ IMPORT:")
        
        for location_type in [LocationUnitType.STATE, LocationUnitType.COUNTY, LocationUnitType.CITY]:
            self.cursor.execute(
                "SELECT COUNT(*) FROM location_unit WHERE type = %s",
                (location_type.value,)
            )
            count = self.cursor.fetchone()[0]
            if count > 0:
                print(f"  {location_type.value.upper()}: {count}")
        
        # Hiển thị cấu trúc mẫu
        print("\n📋 CẤU TRÚC DỮ LIỆU MẪU:")
        self.cursor.execute("""
            SELECT s.name as state_name, c.name as county_name, ci.name as city_name
            FROM location_unit s
            LEFT JOIN location_unit c ON c.parent_id = s.id AND c.type = 'county'
            LEFT JOIN location_unit ci ON ci.parent_id = c.id AND ci.type = 'city'
            WHERE s.type = 'state'
            LIMIT 3
        """)
        
        for row in self.cursor.fetchall():
            state_name, county_name, city_name = row
            print(f"  {state_name} -> {county_name} -> {city_name}")
        
        print("\n💡 LƯU Ý:")
        print("  • Independent Cities (Virginia) cũng được lưu như COUNTY")
        print("  • Tất cả Cities/Towns/Villages đều được lưu như CITY")
        print("  • Cấu trúc: STATE (cấp 1) -> COUNTY (cấp 2) -> CITY (cấp 3)")
    
    def close(self):
        """Đóng kết nối database"""
        self.cursor.close()
        self.conn.close()

# Hàm chính để chạy import
def main():
    try:
        importer = USLocationImporter()
        
        # Import dữ liệu với giới hạn
        # limit_states: số bang muốn import
        # limit_counties: số counties per state  
        # limit_cities: số cities per county
        importer.import_all_data()
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    finally:
        if 'importer' in locals():
            importer.close()

if __name__ == "__main__":
    main()