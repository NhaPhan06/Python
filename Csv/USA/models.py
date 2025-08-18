import uuid
from datetime import datetime
from sqlalchemy import Column, String, Enum, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()

class LocationUnitType(str, enum.Enum):
    STATE = "state"
    CITY = "city"
    COUNTY = "county"
    DISTRICT = "district"
    WARD = "ward"

class LocationUnit(Base):
    __tablename__ = "location_unit"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    type = Column(Enum(LocationUnitType), nullable=False)
    country_id = Column(String, ForeignKey("country.id"), nullable=False)
    parent_id = Column(String, ForeignKey("location_unit.id"), nullable=True)
    
    
class BaseCounty(Base):
    __tablename__ = "base_county"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    type = Column(Enum(LocationUnitType), nullable=False)
    state_name = Column(String, nullable=False)
    


def __repr__(self):
        return f"LocationUnit(name='{self.name}', type='{self.type}', code='{self.code}', parent_code='{self.parent_code}')"
