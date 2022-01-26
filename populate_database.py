import pandas as pd
import pymongo

from utils import is_container_running


def get_collection(host, port):
    '''
    connect then create db and collection to put the data in
    host: Address ip of mongodb server
    port: mongodb server port
    return: Collection
    '''
    is_container_running('mymongo')
    client = pymongo.MongoClient(host, port)
    db = client['mydata']
    return db.online_retail


def read_excel(file_name):
    '''
    read xlsx file and convert data to dictionary
    file_name: target file name
    return : dict
    '''
    df = pd.read_excel(file_name)
    data = df.to_dict('records')
    return data


if __name__ == '__main__':
    collection = get_collection('localhost', 27017)
    data = read_excel("data/online_retail.xlsx")
    collection.insert_many(data)
