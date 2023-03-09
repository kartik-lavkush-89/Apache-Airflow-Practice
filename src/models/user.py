from pydantic import BaseModel


class Detail(BaseModel):
    dag : str
    name:str
    age:int    
    
class OTP(BaseModel):
    dag : str
    phone : int

class File(BaseModel):
    dag : str
    file : str