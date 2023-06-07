import requests
import json
import Config
import psycopg2

url_get = Config.url_get
url_login = Config.url_login

payload = {}
headers = {}
response_token = requests.request("GET", url_login, headers=headers, data=payload)
response_token_d = json.loads(response_token.text) #получаем словарь
response_token_str = response_token_d.setdefault("accessToken", None) #получаем значение ключа
token = 'Bearer'+" "+ response_token_str #токен готов

## Загрузка данных в форму (пример форма по классу "7.02", включая данные из реестра "action_territories")
par = ['7.02']
def insert_main_data(par):
    LIMIT = 100
    attr_7_02 = f'id, guid, class4332a_id, doc_status, doc_name, doc_num, prev_data_registry_id, org_name, territory_id, for_placement_registry_id, number, doc_date, reg_date, attachment_ids, territory_id' #- второй "territory_id" это ссылка на реестр
    class4332a_id_7_02 = f'7.02'
    def table(res):
        a = []
        for i in result:
            a.append({"leafProviderId": 8, "values": {"32": i[0], "33": i[1], "34": i[2], "37": i[3], "38": i[4], "39": i[5], "41": i[6], "42": i[7], "43": i[8], "45": i[9], "46": i[10], "52": i[11].strftime('%d.%m.%Y'), "53": i[12].strftime('%d.%m.%Y'), "59": i[13], "58": i[14]}})
        payload = json.dumps({"reportId": 8, "userGroupId": 1, "stageStatus": "T", "rows": a})
        headers = {'Authorization': token, 'Content-Type': 'application/json'}
        response = requests.request("POST", url_get, headers=headers, data=payload)
        #return print(response.text)

    connection = psycopg2.connect(user=Config.user_post, password=Config.password_post, host=Config.host_post, port=Config.port_post, database=Config.database_post)
    cursor = connection.cursor()
    postgr_select = f"SELECT {attr_7_02} FROM data_registry WHERE class4332a_id ='{class4332a_id_7_02}' ORDER BY id ASC LIMIT {LIMIT}" #получаем первую часть данных == LIMIT
    cursor.execute(postgr_select)
    result = cursor.fetchall()
    table(result)
    #print(result)
    OFFSET = LIMIT
    a = True
    while a:
        postgr_select1 = f"SELECT {attr_7_02} FROM data_registry WHERE class4332a_id ='{class4332a_id_7_02}' ORDER BY id ASC LIMIT {LIMIT} OFFSET {OFFSET}"
        cursor.execute(postgr_select1)
        result = cursor.fetchall()
        table(result)
        OFFSET = OFFSET+LIMIT
        if len(result) == 0:
            a = False
    return print('Данные выгружены в форму')
insert_main_data(par[0])

##Загрузка данных в реестр ("action_territories")
LIMIT = 1000
def table(res):
    a = []
    for i in result:
        a.append({"leafProviderId": 9, "values": {"54": str(i[0]), "55": i[1], "57": i[2]}})
    payload = json.dumps({"reportId": 10, "userGroupId": 1, "stageStatus": "T", "rows": a})
    headers = {'Authorization': token, 'Content-Type': 'application/json'}
    response = requests.request("POST", url_get, headers=headers, data=payload)
    #return print(response.text)

attr_action_terr = f'id, title, parent_territory_id'
connection = psycopg2.connect(user=Config.user_post, password=Config.password_post, host=Config.host_post, port=Config.port_post, database=Config.database_post)
cursor = connection.cursor()
postgr_select = f"SELECT {attr_action_terr} FROM action_territories ORDER BY id ASC LIMIT {LIMIT}" #получаем первую часть данных == LIMIT
cursor.execute(postgr_select)
result = cursor.fetchall()
table(result)

OFFSET = LIMIT
a = True
while a:
    postgr_select1 = f"SELECT {attr_action_terr} FROM action_territories ORDER BY id ASC LIMIT {LIMIT} OFFSET {OFFSET}"
    cursor.execute(postgr_select1)
    result = cursor.fetchall()
    table(result)
    OFFSET = OFFSET+LIMIT
    if len(result) == 0:
        a = False
print('Данные выгружены в реестр')













