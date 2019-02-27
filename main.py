from publish import PublishService
from flask import Flask, Response, request
from waitress import serve
import pymongo, json, logging, pprint, datetime

# module logger
module_logger = logging.getLogger('UserService.main')
module_logger.setLevel(logging.ERROR)
ch = logging.FileHandler('UserService.log')
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
module_logger.addHandler(ch)

# flask server
app = Flask(__name__)


class UserService:
    _conn = None
    publisher = None
    DB_NAME = 'UserDatabase'

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('UserService.main.UserService')
        self.logger.info('creating an instance of UserService')
        return super().__init__(*args, **kwargs)
    
    # def __exit__(self):
    #     self.logger.info('deleting an instance of UserService.main.UserService')
    #     if self._conn:
    #         self._conn.close()
    #     self.logger.info('deleted an instance of UserService.main.UserService')

    def get_mongo_client(self):
        # lazy singleton service
        if self._conn:
            return self._conn
        else:
            self.connect()
            return self._conn
    
    def get_publisher(self):
        # lazy singleton service
        if self.publisher:
            return self.publisher
        else:
            self.publisher = PublishService()
            return self.publisher

    def connect(self):
        """
        connects to a mongoDB cluster db for this microservice using the `SRVAdd` in db_config.json
        """
        mongo_cluster_data = open('db_config.json').read()
        data = json.loads(mongo_cluster_data)
        self.logger.info('connecting to database.')
        self._conn = pymongo.MongoClient(host=data['SRVAdd'])
        self.logger.info('connected to database.')
    
    def list_databases(self):
        """Returns all databases info in our mongodb cluster
        
        Returns:
            list of dicts -- databases_info
        """

        _conn = self.get_mongo_client()
        return [i for i in _conn.list_databases()]
    
    def get_database(self, name):
        self.logger.info(f'getting database, {name}')
        _conn = self.get_mongo_client()
        db = _conn[name]
        self.logger.info(f'returning database, {name}')
        return db
    
    def get_or_create_collection(self, coll_name):
        """lazily get or create a collection from our db
        
        Arguments:
            db {MongoClient} -- db returned through self.get_database(name)
            coll_name {str} -- name of the collection
        
        Returns:
            [collection] -- [curr pointing to the collection]
        """
        db = self.get_database(self.DB_NAME)
        self.logger.info(f'getting or lazily creating a collection {coll_name}')
        coll = db[coll_name]
        self.logger.info(f'done getting or lazily creating a collection {coll_name}')
        return coll

    def create_user(self, coll_name='Users', **kwargs):
        data = {**kwargs}
        str_data = json.dumps(data)
        
        self.logger.info(f'inserting {str_data} into {coll_name}')
        coll = self.get_or_create_collection(coll_name)
        inserted = coll.insert_one({**kwargs})
        self.logger.info(f'inserted {str_data} into {coll_name}')
        
        self.logger.info(f'publishing {str_data} using fanout')
        self.get_publisher().publish_new_user_created(**data)
        self.logger.info(f'published {data} using fanout')
        return inserted

# if __name__ == "__main__":
#     a = UserService()
#     print(a.create_user(username="Sony Ujjawal", password="qazwsxec", name="khushi ujjawal"))
#     print(a.create_user(username="Nirmala Ujjawal", password="qazwsxec", name="khushi ujjawal"))

user_service = UserService()

@app.route('/', methods=['POST'])
def create_user():
    if request.method == 'POST':
        user_service.create_user(**request.form)
        return Response('{ "created": 1 }', status=201, mimetype='application/json')


serve(app, port=8080)

