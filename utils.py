import pandas as pd
import pymongo as pym
import json

class mongo():
    
            
    def connect(self, db, collection):
       
       myclient = pym.MongoClient('mongodb://localhost:27017/')
       mydb = myclient[db] 
       
       return mydb[collection]
   
    def create_collections(self, db, collection_names, PATH):
        
        for coll in collection_names:
            
            mycol = self.connect(db, coll)
            with open(PATH + coll + '.json') as f:
                data = json.load(f)
            mycol.insert_many(data)
   
    def query(self, query, prnt = True):
        
        if prnt:
            
            df = pd.DataFrame(query)
            print(df.head(15).to_string(index=False))
            return df
        else:
            return pd.DataFrame(query)
    

        
        
        
          
        



