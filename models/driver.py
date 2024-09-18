from pydantic import BaseModel

class Driver(BaseModel):
    name: str
    driver_id: int
