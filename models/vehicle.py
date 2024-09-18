from pydantic import BaseModel

class Vehicle(BaseModel):
    vid: int
    regno: str
