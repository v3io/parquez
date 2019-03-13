import requests
from pyhive import presto

NODE_IP = '192.168.220.23'
USER_NAME = 'admin'
PASSWORD = '24tango'
LOG_LEVEL = 'info'
NODE_PORT = '8001'


def generate_url():
    url = "http://" + NODE_IP + ":" + NODE_PORT
    return url


def create_cookie():
    url = generate_url() + "/api/sessions"
    headers = {"Content-Type": "application/json"}
    payload = {
        "data": {
            "attributes": {
                "username": USER_NAME,
                "password": PASSWORD
            },
            "type": "session"
        }
    }
    response = requests.post(url, json=payload, headers=headers)
    print(response.headers)
    print(response.text)
    for c in response.cookies:
        print(c.name, c.value)
    return c.value


req_kw = {'auth': ("iguazio", create_cookie()), 'verify': False}
conn = presto.connect("presto-api-presto.default-tenant.app.dev133.lab.iguazeng.com", port=8080, username='iguazio', protocol='https', requests_kwargs=req_kw)

query = """
select
  * from v3io.parquez.booking_service_kv limit 10"""

col = ['id', 'name']
cursor = conn.cursor()
cursor.execute(query)
print(cursor.fetchall())
cursor.close()