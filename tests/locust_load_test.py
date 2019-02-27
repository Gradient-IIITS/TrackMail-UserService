from locust import HttpLocust, TaskSet
from faker import Faker

# locust --host=http://127.0.0.1:8000 -f tests/locust_load_test.py 

fake = Faker()

def create_profile(l):
    # l.client.get("/")
    l.client.post("/", 
        {
            "name": fake.name(),
            "address": fake.address(),
            "ssn": fake.itin(),
            "first_name": fake.first_name_male(),
            "last_name": fake.last_name_male()
        })

class UserBehavior(TaskSet):
    tasks = {create_profile: 1}

    def on_start(self):
        pass

    def on_stop(self):
        pass

class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 5000
    max_wait = 9000