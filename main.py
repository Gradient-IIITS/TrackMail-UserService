from publish import PublishService
from flask import Flask, Response, request
from waitress import serve
from passlib.hash import pbkdf2_sha256
import pymongo, json, logging, pprint, datetime, re

# module logger
module_logger = logging.getLogger('UserService.main')
module_logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('UserService.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
module_logger.addHandler(ch)

# flask server
app = Flask(__name__)


class UserService:
    _conn = None
    _publisher = None
    _required_fields = ['username', 'first_name', 'last_name', 'email', 'password']
    _username_min_len = 8
    EMAIL_RE = re.compile(r'^[a-zA-Z\d\._\+-]+@([a-z\d-]+\.?[a-z\d-]+)+\.[a-z]{2,4}$')
    DB_NAME = 'UserDatabase'

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('UserService.main.UserService')
        self.logger.info('creating an instance of UserService')
        return super().__init__(*args, **kwargs)

    def get_mongo_client(self):
        # lazy singleton service
        if self._conn:
            return self._conn
        else:
            self.connect()
            return self._conn
    
    def get_publisher(self):
        # lazy singleton service
        if self._publisher:
            return self._publisher
        else:
            self._publisher = PublishService()
            return self._publisher

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

    #TODO: [improve logging]
    def validate_data(self, data):
        self.logger.info(f'checking errors in validate_data')
        keys = data.keys()
        errors = []
        for field in self._required_fields:
            if field not in keys:
                errors.append({field: "this field is required"}) 
        
        # matching email
        if 'email' in keys:
            match = re.match(self.EMAIL_RE, data['email'])
            if match is None:
                errors.append({"email": "invalid email address"})
        
        # matching username
        if 'username' in keys and len(data['username']) < self._username_min_len:
            errors.append({"username": f"minlength {self._username_min_len} characters"})
        
        self.logger.info(f'returning errors in validate_data')
        return errors
    
    def hash_password(self, password):
        return pbkdf2_sha256.hash(password)
    
    def match_password(self, password, hash):
        return pbkdf2_sha256.verify(password, hash)

    def create_user(self, coll_name='Users', **kwargs):
        data = {**kwargs}
        errors = self.validate_data(data)
        if errors:
            return errors
        # hashing password before insertion
        data["password"] = self.hash_password(data["password"])
        str_data = json.dumps(data)
        
        self.logger.info(f'inserting {str_data} into {coll_name}')
        coll = self.get_or_create_collection(coll_name)
        inserted = coll.insert_one({**kwargs})
        self.logger.info(f'inserted {str_data} into {coll_name}')
        self.get_publisher().publish_new_user_created(**data)
        return data


user_service = UserService()

@app.route('/', methods=['POST'])
def create_user():
    if request.method == 'POST':
        to_insert = request.form.to_dict()
        inserted_data_or_err = user_service.create_user(**to_insert)
        return Response({json.dumps(inserted_data_or_err)}, status=201, mimetype='application/json')

serve(app, port=8080)

