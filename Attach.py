import Config
import psycopg2
import sqlite3
import os
from pathlib import Path
import urllib.request
import urllib
from urllib.parse import unquote, quote
import os
import zipfile


# 31.05.2023 пренебрегаем кодировкой кирилицы, запись списка конечных файлов из zip архивов в базу данных и в log.csv +скачивание файлов (код в части скачивания НЕ ПРОВЕРЕН!!! нет доступа)
# Часть 1 записываем id (data reg), guid (data reg), attach (attaches), путь до файла.
def create_bd(*args):
    global conn
    conn = sqlite3.connect('log.db')  # создаем файл log.db
    global  cur
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS log (id INT, guid TEXT, attach TEXT, path TEXT)""") # не сделала отметку об обработке строки
    cur.execute(*args)
    conn.commit()
    #conn.close()
my_file = Path("C:\\Users\\79298\\PycharmProjects\\pythonProject9\\log.csv")
if my_file.is_file():
    with open('log.csv', 'r', encoding='utf-8') as f:
        last_line = f.readlines()[-1]  # получили последнюю записанную строку
        last_line_list = last_line.split(';')  # перевели в лист
        end_id_str = last_line_list[0]  # получили id в строковом значении
        #print(end_id_str)
        end_id_int = int(end_id_str)  # id в int
else:
    end_id_int = 0
    with open('log.csv', 'a', encoding='utf-8') as f:
        f.write("doc_id" + ';' + "doc_guid" + ';' + "atach_id" + ';' + "path" + '\n')  # задали вернюю строчку таблицы "наименование столбцов"
        f.write("0" + ';' + "0" + ';' +  "0" + ';' + "0" + '\n')

postgr_select = f"SELECT id, guid, class4332a_id, doc_name, territory_id, attachment_ids, number, version_number, updated_at, is_actual FROM data_registry WHERE class4332a_id in ('3.01','3.02','3.03','2.02','2.02') and id >= {int(end_id_int)} and is_actual = 'true' ORDER BY id ASC " #limit 200 # получаем все по 3 и 2 классу

def csv_bd_upload(upload):
    connection = psycopg2.connect(user=Config.user_post, password=Config.password_post, host=Config.host_post, port=Config.port_post, database=Config.database_post)
    cursor = connection.cursor()
    postgr_select = upload
    cursor.execute(postgr_select)
    docs = cursor.fetchall() #[(...[]....), (...[]...),(...[]...)]
    for doc in docs:
        atachs = doc[5]  # из кортежей выбрали 'attach' ['....'] ['....', '....', '....']
        for atach in atachs:
            postgr_select1 = f"SELECT id, path, file, updated_at FROM public.attachments WHERE id = \'{atach}\'  "
            cursor.execute(postgr_select1)
            atach_result = cursor.fetchall()  # [('attach', '...', ..., 'путь к файлу', 'название файла.tif?63842857964', update datetime)]
            # print(atach_result)
            path = atach_result[0][1] + '/' + atach_result[0][2]  # '2023-02-11/6c3fe323-e5d0-4afd-bb0b-5cc8e1a98377/03601101001-ФЗ-2018._modified.tif?63843335996'
            if (path.find('.zip') != -1) or (path.find('.ZIP') != -1):
                dir_path = r'\DF-DSM\share\uploads'
                path1 = path.split('?') # ['2022-12-11/46fb1345-39c5-475d-89b1-9212404d008a/ТОМ 1.1 ГП  ст.Новоивановской.zip', '63838020009']
                print(dir_path)
                print(path1[0])
                print(dir_path + path1[0])
                with zipfile.ZipFile(dir_path + path1[0]) as archive:
                    for entry in archive.infolist():
                        name = entry.filename.encode('cp437').decode('cp437')  # 866 -кириллица
                        file_path = os.path.join(path1[0], *name.split('/'))
                        print(file_path)
                        if file_path[-1]=='\\':
                            continue
                        else:
                            with open('log.csv', 'a', encoding='utf-8') as f:
                                f.write(str(doc[0]) + ';' + doc[1] + ';' + atach + ';' + file_path + '\n')
                            create_bd("INSERT INTO log VALUES (?, ?, ?, ?)", (str(doc[0]), doc[1], atach, file_path))
            else:
                with open('log.csv', 'a', encoding='utf-8') as f:
                    f.write(str(doc[0]) + ';' + doc[1] + ';' + atach + ';' + path.split('?')[0] + '\n')
                create_bd("INSERT INTO log VALUES (?, ?, ?, ?)", (str(doc[0]), doc[1], atach, path.split('?')[0]))
                continue

csv_bd_upload(postgr_select)