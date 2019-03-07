import pika, json
import json, logging, pprint, datetime

# module logger
module_logger = logging.getLogger('UserService.publish')
module_logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('UserService.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
module_logger.addHandler(ch)


class PublishService:
    _conn = None
    TOPIC_NAME = 'UsersUpdate'
    p_type = {
        "create": "CREATE",
        "module": "USER_SERVICE"
    }

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger('UserService.publish.PublishService')
        self.logger.info('creating an instance of PublishService')
        return super().__init__(*args, **kwargs)
    
    def get_mq_client(self):
        # lazy singleton service
        if self._conn:
            return self._conn
        else:
            self.connect()
            return self._conn
    
    def drop_mq_client(self):
        self.logger.info('closing connection to rabbitMQ.')
        if self._conn:
            self._conn.close()
        self._conn = None
        self.logger.info('closed connection to rabbitMQ.')

    def connect(self):
        """
        connects to a rabbitMQ for this microservice using config.json
        """
        # rabbit_mq_data = open("config_local.json").read()
        rabbit_mq_data = open("config.json").read()
        data = json.loads(rabbit_mq_data)
        self.logger.info('connecting to rabbitMQ.')
        parameters = pika.URLParameters(data["url"])
        self._conn = pika.BlockingConnection(parameters)
        self.logger.info('connected to rabbitMQ.')

    def publish_new_user_created(self, *args, **kwargs):
        """
        Publish a creation of new user in the database.
        """
        data = {**kwargs, **self.p_type}
        str_data = json.dumps(data)
        try:
            self.logger.info(f'publishing {str_data} into Queues by fanout')
            channel = self.get_mq_client().channel()
            channel.exchange_declare(exchange=data["module"], exchange_type='fanout')
            channel.basic_publish(exchange=data["module"], routing_key='', body=str_data)
            self.logger.info(f'published {str_data} into Queues')
        except Exception as e:
            self.logger.info(f'failed publishing {str_data} Queues by fanout.')
            self.logger.error(e, exc_info=True)
            self.logger.info(f'trying publishing {str_data} Queues by fanout again.')

            # heartbeat problem.
            self.drop_mq_client()
            self.connect()

            self.logger.info(f'publishing {str_data} into Queues by fanout')
            channel = self.get_mq_client().channel()
            channel.exchange_declare(exchange=data["module"], exchange_type='fanout')
            channel.basic_publish(exchange=data["module"], routing_key='', body=str_data)
            self.logger.info(f'published {str_data} into Queues')


# if __name__ == "__main__":
#     a = PublishService()
#     a.connect()