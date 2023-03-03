from pydantic import BaseModel


class Detail(BaseModel):
    dag : str
    name:str
    age:int    
