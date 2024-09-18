from pydantic import BaseModel
from helpers.enums import UserType

class User(BaseModel):
    name: str
    type: UserType