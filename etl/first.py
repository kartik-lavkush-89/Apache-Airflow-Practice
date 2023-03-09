import random
import string
import pandas as pd
from pymongo import MongoClient

conn = MongoClient()

def gen_ran(n,destination_file_name):
    ids=[]
    names=[]
    classes=[]
    for i in range(n):
        id=random.randint(0,1000)
        nm = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=10))
        cls=random.randint(1,13)
        ids.append(id)
        names.append(nm)
        classes.append(cls)
    df=pd.DataFrame()
    df["id"]=ids
    df["name"]=names
    df["class"]=classes
    df.to_csv(f"data/{destination_file_name}",index=False)




def load_to_mongo(file_name):
    df = pd.read_csv(f'data/{file_name}')
    records = df.to_dict('records')
    # conn.csv.data.insert_many(records)
    print("added")
    return df.to_string()

# gen_ran(10,"hisfile.csv")

# load_to_mongo("hisfile.csv")