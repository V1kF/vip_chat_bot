
import requests # запросы из кода
import json # формат данных
import threading # потоки
import pymysql # работа с бд
from payment_form import * # настройки формы оплаты
import re # регулярные выражения
import time # работа со временем,конкретно, в юнит формате
import random # для рандомной генерации чисел

#токен бота
TOKEN='bot token'

#айди канала
CHANNEL_CHAT_ID='id channel'

URL='https://api.telegram.org/bot'

# киви токен и ключи
QIWI_TOKEN='qiwi token'
QIWI_PUBLIC_KEY='p2p public key'
QIWI_SECRET_KEY='p2p public key'

#данные бд
DB_NAME='vip_chat'
TABLE_NAME='vip_chat_bot'
PASSWORD='mysql'
HOST='localhost'
USER='mysql'

#-----------------------------------------

#айди сообщений
REF_LINK_MESSAGE_ID={}
SUBSCRIPTION_MESSAGE_ID={}
CHECK_MESSAGE_ID={}
BALANCE_MESSAGE_ID={}
PAYMENT_KIWI_MESSAGE_ID={}
PAYOUTS_MESSAGE_ID={}
PAYOUT_MESSAGE_ID={}

#баланс и реквизитные данные пользователей
BALANCE_KIWI=[]

#время подписки
SUBSCRIPTION_TIME=''

#генерируемый номер счёта
UNIQUE_STR=''

#данные о подписчиках канала
SUBSCRIBERS_CHANNEL=[]

#отправка сообщений
def send_message(chat_id, text,reply_markup='',entities=''):
    answer_str=requests.get(f'{URL}{TOKEN}/sendMessage?chat_id={chat_id}&text={text}&reply_markup={reply_markup}&entities={entities}')
    answer_str = answer_str.text
    answer = json.loads(answer_str)  # переделываем в словарь
    #print(answer)
    return answer

def payment_form(billid,currency,value,expirationDateTime):

    global QIWI_SECRET_KEY

    billid = billid

    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer ' + QIWI_SECRET_KEY,
        'content-type': 'application/json'
    }
    params = {
        'amount': {
            'currency': currency,
            'value': value
        },
        'expirationDateTime': expirationDateTime
    }

    url = f'https://api.qiwi.com/partner/bill/v1/bills/{billid}'

    resp = requests.put(url, json=params, headers=headers)

    print(resp.text)

    return  resp

#запрос на получение событий,которые приходят боту
receiving_messages_str = requests.get(f'{URL}{TOKEN}/getUpdates?')
receiving_messages_str = receiving_messages_str.text
receiving_messages = json.loads(receiving_messages_str)

print(receiving_messages)

if receiving_messages['ok'] and receiving_messages['result']!=[]:

    update_id=receiving_messages['result'][-1]['update_id']

def check_subscription():

    while True:
        try:

            connection = pymysql.connect(
                host=HOST,
                port=3306,
                user=USER,
                password=PASSWORD,
                database=DB_NAME,
                cursorclass=pymysql.cursors.DictCursor
            )

            with connection.cursor() as cursor:

                time.sleep(2)

                # поиск id и времени подписки и флажка уведомления человека в базе
                cursor.execute(f"SELECT id,vip_chat,notification FROM {TABLE_NAME} WHERE vip_chat!=0;")
                connection.commit()

                SUBSCRIBERS_CHANNEL=cursor.fetchall()

                if cursor.fetchall() != ():  # если человек/люди с подпиской найден/ы

                    for subscriber in SUBSCRIBERS_CHANNEL:

                        time_now=time.time()

                        time_now = str(time_now)

                        if re.fullmatch(r"(.+)\..+", time_now):

                            time_now = re.findall(r"(.+)\..+", time_now)

                            time_now=int(time_now[0])

                        if subscriber['vip_chat']-86400<=time_now and subscriber['notification']==0:#если время подписки-сутки больше или равно времени сейчас уведомляем об оплате

                            send_message(subscriber['id'],"*Не забудьте продлить подписку, иначе будете исключены из канала*")

                            cursor.execute(f"UPDATE {TABLE_NAME} SET notification=1 WHERE id={subscriber['id']};")
                            connection.commit()

                        if subscriber['vip_chat']<=time_now:#если время подписки больше или равно времени сейчас запрещаем доступ к каналу

                            ban_str = requests.get(f'{URL}{TOKEN}/banChatMember?chat_id={CHANNEL_CHAT_ID}&user_id={subscriber["id"]}')
                            ban_str = ban_str.text
                            ban = json.loads(ban_str)

                            cursor.execute(f"UPDATE {TABLE_NAME} SET vip_chat=0 WHERE id={subscriber['id']};")
                            connection.commit()

                else:

                    pass

            connection.close()

        except Exception as ex:
            print("Ошибка в потоке check_subscription")
            print(ex)

thread=threading.Thread(target=check_subscription,args=(SUBSCRIBERS_CHANNEL))

thread.start()

def ban_freeloader():#бан людей которые перешли по ссылке не оплатив подписку


    while True:

        time.sleep(1)

        try:

            if receiving_messages['ok'] and receiving_messages['result']!=[]:

                connection = pymysql.connect(
                    host=HOST,
                    port=3306,
                    user=USER,
                    password=PASSWORD,
                    database=DB_NAME,
                    cursorclass=pymysql.cursors.DictCursor
                )
                 #берём из бд людей с подпиской
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT id FROM {TABLE_NAME} WHERE vip_chat!=0;")
                    connection.commit()

                    honest_persons=cursor.fetchall()

                connection.close()

                 #запрос на получение событий,которые приходят боту
                receiving_messages_str2 = requests.get(f'{URL}{TOKEN}/getUpdates?')
                receiving_messages_str2 = receiving_messages_str2.text
                receiving_messages2 = json.loads(receiving_messages_str2)

                users = receiving_messages2['result']

                ban = True

                for user in users:#перебираем людей совершивших события

                    if user.get('message') and user['message'].get('new_chat_members') and\
                            user['message']['new_chat_members']['username'] != 'crypt_for_a_secret_bot':#если пользователь является втупившим

                        for honest_person in honest_persons:

                            if honest_person['id']==user['message']['new_chat_members']['id']:#если человек один из приобрётших людей подписку

                                ban=False#ставим фолз,что значит что человек заплатил

                        if  ban==True:#если человеку скинули ссылку и он по ней перешёл не заплатив,баним его

                            ban_str = requests.get(f'{URL}{TOKEN}/banChatMember?chat_id={CHANNEL_CHAT_ID}&user_id={user["message"]["new_chat_members"]["id"]}')
                            ban_str = ban_str.text
                            ban_pers = json.loads(ban_str)

        except Exception as ex:

            print("Ошибка в потоке ban_freeloader")
            print(ex)

thread = threading.Thread(target=ban_freeloader, args=())

thread.start()

while True:

    try:

        if receiving_messages['ok'] and receiving_messages['result']!=[]:

            # запрос на получение событий,которые приходят боту
            receiving_messages_str2 = requests.get(f'{URL}{TOKEN}/getUpdates?')
            receiving_messages_str2 = receiving_messages_str2.text
            receiving_messages2 = json.loads(receiving_messages_str2)

            events = receiving_messages2['result']

            for event in events:

                if update_id<event['update_id']:

                    update_id=event['update_id']

                    if  event.get('message'):

                        id_pers = event['message']['from']['id']

                        connection = pymysql.connect(
                            host=HOST,
                            port=3306,
                            user=USER,
                            password=PASSWORD,
                            database=DB_NAME,
                            cursorclass=pymysql.cursors.DictCursor
                        )

                        with connection.cursor() as cursor:

                            # поиск id человека в базе

                            cursor.execute(f"SELECT id FROM {TABLE_NAME} WHERE id= {id_pers};")
                            connection.commit()

                            if cursor.fetchall() != ():  # если человек с таким id найден
                                pass

                            else:

                                # добавление id в базу данных
                                cursor.execute(f"INSERT INTO {TABLE_NAME} (id) VALUE ({id_pers});")
                                connection.commit()

                        connection.close()

                    if 'callback_query' in event:  # Если нажата кнопка

                        data=event['callback_query']['data'] # данные о кнопке

                        id_pers=event['callback_query']['from']['id']

                        chat_id=event['callback_query']['message']['chat']['id']

                        if data=='ref_link':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            if REF_LINK_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={REF_LINK_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )

                            with connection.cursor() as cursor:

                                cursor.execute(f"UPDATE {TABLE_NAME} SET menu=1 WHERE id={id_pers};")
                                connection.commit()

                                # поиск собственной реф ссылки человека в базе
                                cursor.execute(f"SELECT promo_pers FROM {TABLE_NAME} WHERE id= {id_pers};")
                                connection.commit()
                                print(cursor.fetchall())

                                if cursor.fetchall() != ():

                                    # добавляем в графу созданную только что реф ссылку(указываем 1 что она в принципе создана)
                                    cursor.execute(f"UPDATE {TABLE_NAME} SET promo_pers=1 WHERE id={id_pers};")
                                    connection.commit()

                            connection.close()

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            REF_LINK_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                            f"Ваша ссылка - приглашение: https://t.me/crypt_for_a_secret_bot?start={id_pers}")['result']['message_id'])

                        if data == 'subscription':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            unique_str = str(id_pers) + str(event['update_id']) +\
                                         str(random.randint(1, 100000000000000000000000000000000000000000))

                            if SUBSCRIPTION_MESSAGE_ID.get(f'{id_pers}')!=None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={SUBSCRIPTION_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)


                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )

                            with connection.cursor() as cursor_0:

                                cursor_0.execute(f"UPDATE {TABLE_NAME} SET menu=2 WHERE id={id_pers};")
                                connection.commit()

                            with connection.cursor() as cursor_1:

                                # добавляем в графу номер счёта
                                cursor_1.execute(f"UPDATE {TABLE_NAME} SET billid='{unique_str}' WHERE id={id_pers} AND billid='0';")
                                connection.commit()

                                # берём номер счёта
                                cursor_1.execute(f"SELECT billid FROM {TABLE_NAME} WHERE id= {id_pers};")
                                connection.commit()

                                UNIQUE_STR=cursor_1.fetchall()[0]['billid']

                            with connection.cursor() as cursor_2:

                                # берём время подписки
                                cursor_2.execute(f"SELECT vip_chat FROM {TABLE_NAME} WHERE id= {id_pers};")
                                connection.commit()

                                vip_chat=cursor_2.fetchall()[0]['vip_chat']

                            connection.close()

                            url = payment_form(UNIQUE_STR, CURRENCY, AMOUNT_VALUE, EXPIRATIONDATETIME)

                            url = url.text

                            url = json.loads(url)

                            if vip_chat==0:

                                json_format = {"inline_keyboard": [[{"text": "перейти к оплате💳",
                                                                     "url": f"{url['payUrl']}"}],[{"text": "Я купил подписку!✔",
                                                                     "callback_data": "check"}]]}

                                json_format = json.dumps(json_format)

                                SUBSCRIPTION_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                "1)Купите подписку\n2)Нажмите на кнопку 'Я купил подписку!'\n3)Переходите на канал ",
                                                                             json_format)['result']['message_id'])

                            else:
                                json_format = {"inline_keyboard": [[{"text": "продлить💳", "url": f"{url['payUrl']}"}],
                                                                   [{"text": "Я продлил подписку!✔", "callback_data": "check"}]]}

                                json_format = json.dumps(json_format)

                                SUBSCRIPTION_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                "1)продлите подписку\n2)Нажмите на кнопку 'Я продлил подписку!'\n3)Переходите на канал ",
                                                                                 json_format)['result']['message_id'])

                        if data == 'check':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            if CHECK_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={CHECK_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )

                            with connection.cursor() as cursor:

                                cursor.execute(f"UPDATE {TABLE_NAME} SET menu=3 WHERE id={id_pers};")
                                connection.commit()

                                # берём время действия подписки
                                cursor.execute(f"SELECT vip_chat FROM {TABLE_NAME} WHERE id= {id_pers} AND vip_chat!=0;")
                                connection.commit()

                                SUBSCRIPTION_TIME=cursor.fetchall()

                                if cursor.fetchall()!=():

                                    #генерация временной ссылки на канал
                                    invite_link_str = requests.get(f'{URL}{TOKEN}/createChatInviteLink?chat_id={CHANNEL_CHAT_ID}')
                                    invite_link_str = invite_link_str.text
                                    invite_link = json.loads(invite_link_str)
                                    print(invite_link)

                                    CHECK_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                    f"Переходите на канал 👉🏻 {invite_link['result']['invite_link']}")['result']['message_id'])

                                # берём номер счёта
                                cursor.execute(f"SELECT billid FROM {TABLE_NAME} WHERE id= {id_pers};")
                                connection.commit()

                                UNIQUE_STR=cursor.fetchall()[0]['billid']

                            connection.close()

                            headers = {
                                'accept': 'application/json',
                                'Authorization': 'Bearer ' + QIWI_SECRET_KEY,
                            }

                            url = f'https://api.qiwi.com/partner/bill/v1/bills/{UNIQUE_STR}'

                            resp = requests.get(url,headers=headers)

                            resp=resp.text

                            resp=json.loads(resp)

                            if resp["status"]["value"]=="PAID":

                                #берём информацию об участнике
                                infoChatMember_str = requests.get(f'{URL}{TOKEN}/getChatMember?chat_id={CHANNEL_CHAT_ID}&user_id={id_pers}')
                                infoChatMember_str = infoChatMember_str.text
                                infoChatMember = json.loads(infoChatMember_str)

                                if not infoChatMember['result'].get('until_date'):#если человек заблокирован

                                    unban_str = requests.get(f'{URL}{TOKEN}/unbanChatMember?chat_id={CHANNEL_CHAT_ID}&user_id={id_pers}')
                                    unban_str = unban_str.text
                                    unban = json.loads(unban_str)

                                connection = pymysql.connect(
                                    host=HOST,
                                    port=3306,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DB_NAME,
                                    cursorclass=pymysql.cursors.DictCursor
                                )

                                with connection.cursor() as cursor:

                                    #показатель об уведомлении(0-его не было)
                                    cursor.execute(f"UPDATE {TABLE_NAME} SET notification=0 WHERE id={id_pers} AND notification!=0;")
                                    connection.commit()

                                connection.close()

                                # генерация временной ссылки на канал
                                invite_link_str = requests.get(f'{URL}{TOKEN}/createChatInviteLink?chat_id={CHANNEL_CHAT_ID}')
                                invite_link_str = invite_link_str.text
                                invite_link = json.loads(invite_link_str)  # переделываем в словарь

                                CHECK_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                f"Переходите на канал 👉🏻 {invite_link['result']['invite_link']}")['result']['message_id'])

                                now=str(time.time())# время на момент покупки подписки

                                days_30=2592000 #количество секунд в 30 днях

                                if re.fullmatch(r"(.+)\..+", now):

                                    result = re.findall(r"(.+)\..+", now)

                                    connection = pymysql.connect(
                                        host=HOST,
                                        port=3306,
                                        user=USER,
                                        password=PASSWORD,
                                        database=DB_NAME,
                                        cursorclass=pymysql.cursors.DictCursor
                                     )

                                    with connection.cursor() as cursor:

                                        # добавляем в графу вип чат текущее время + месяц в системе юнит
                                        cursor.execute(f"UPDATE {TABLE_NAME} SET vip_chat=vip_chat+{int(result[0])+days_30} WHERE id={id_pers};")
                                        connection.commit()

                                        # присваиваем человеку новый номер счёта дабы избежать того что человек будет продлевать себе подписку по кнопке
                                        cursor.execute(f"UPDATE {TABLE_NAME} SET billid='0' WHERE id={id_pers};")
                                        connection.commit()

                                    connection.close()

                            else: # если время подписки истекло или человек её не покупал

                                if SUBSCRIPTION_TIME==():

                                    CHECK_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                    f"За доступ к каналу надо платить💵✋🏻")['result']['message_id'])

                        if data == 'subscription_date':

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )
                            with connection.cursor() as cursor:

                                cursor.execute(f"UPDATE {TABLE_NAME} SET menu=4 WHERE id={id_pers};")
                                connection.commit()

                                #берём из бд время подписки
                                cursor.execute(f"SELECT vip_chat FROM {TABLE_NAME} WHERE id= {id_pers};")
                                connection.commit()

                                vip_chat=cursor.fetchall()

                            connection.close()

                            if  vip_chat[0]['vip_chat']==0:

                                answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}&text=Вы ещё не преобрели подписку&show_alert=true')
                                answer_str = answer_str.text
                                answer = json.loads(answer_str)

                            else:
                                subscription_date=time.ctime(int(vip_chat[0]['vip_chat']))

                                answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}&text=Вы можете посещать канал\n'
                                                          f'                   до :\n {subscription_date}&show_alert={True}')
                                answer_str = answer_str.text
                                answer = json.loads(answer_str)

                        if data == 'balance':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            if BALANCE_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={BALANCE_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )
                            with connection.cursor() as cursor:

                                cursor.execute(f"UPDATE {TABLE_NAME} SET menu=5 WHERE id={id_pers};")
                                connection.commit()

                                # берём из бд время подписки
                                cursor.execute(f"SELECT balance FROM {TABLE_NAME} WHERE id= {id_pers} AND promo_pers=1;")  # выполняет запрос к базе данных
                                connection.commit()

                                balance=cursor.fetchall()

                                if balance!=():

                                    money=balance[0]['balance']

                                else:
                                    money='отсутствует реф ссылка'

                            connection.close()

                            if money==0:

                                BALANCE_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                f"Ваш баланс составляет:\n {INDEMNITY*int(money)} ")['result']['message_id'])


                            elif money=='отсутствует реф ссылка':

                                BALANCE_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                f"Для начала нужно создать реф-ссылку")['result']['message_id'])


                            else:
                                json_format = {"inline_keyboard": [[{"text": "на киви🥝💶 ", "callback_data": "payment_kiwi"}]]}

                                json_format = json.dumps(json_format)

                                BALANCE_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                f"Ваш баланс составляет:\n {INDEMNITY * int(money)} \n\n"
                                f"Чтобы в дальнейшем вам выплатили эти деньги требуется оставить свой номер киви кошелька",
                                                                                  json_format)['result']['message_id'])

                        if data == 'payment_kiwi':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            if PAYMENT_KIWI_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={PAYMENT_KIWI_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )

                            with connection.cursor() as cursor:

                                cursor.execute(f"UPDATE {TABLE_NAME} SET menu=6 WHERE id={id_pers};")
                                connection.commit()

                            connection.close()

                            PAYMENT_KIWI_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                            f"Введите номер вашего киви кошелька ( 'плюс' тоже указывать )")['result']['message_id'])

                        if data == 'payouts':#кнопка по которой админы и владелец смогут делать выплаты участникам канала

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)  # переделываем в словарь

                            if PAYOUTS_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={PAYOUTS_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)  # переделываем в словарь

                            # берём информацию о участнике
                            infoChatMember_str = requests.get(f'{URL}{TOKEN}/getChatMember?chat_id={CHANNEL_CHAT_ID}&user_id={id_pers}')
                            infoChatMember_str = infoChatMember_str.text
                            infoChatMember = json.loads(infoChatMember_str)  # переделываем в словарь

                            if infoChatMember['result'].get('is_anonymous')!=None:#если человек является владельцем канала или администратором

                                # берём из бд время подписки
                                connection = pymysql.connect(
                                    host=HOST,
                                    port=3306,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DB_NAME,
                                    cursorclass=pymysql.cursors.DictCursor
                                )

                                with connection.cursor() as cursor:

                                    #берём из бд баланс и реквизитные данные человека
                                    cursor.execute(f"SELECT id,balance,kiwi_card FROM {TABLE_NAME} WHERE kiwi_card!=0 AND balance!=0;")  # выполняет запрос к базе данных
                                    connection.commit()

                                    BALANCE_KIWI = cursor.fetchall()

                                connection.close()

                                if BALANCE_KIWI!=():

                                    json_format = {"inline_keyboard": [[{"text": "оплата","callback_data": "payout"}]]}

                                    json_format = json.dumps(json_format)

                                    PAYOUTS_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                    f"Выплата в размере ({INDEMNITY*BALANCE_KIWI[0]['balance']})\n"
                                    f"Пользователю : {BALANCE_KIWI[0]['id']}",json_format)['result']['message_id'])

                                else:

                                    PAYOUTS_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id, f"Все выплаты сделаны")['result']['message_id'])

                        if data == 'payout':

                            answer_str = requests.get(f'{URL}{TOKEN}/answerCallbackQuery?callback_query_id={event["callback_query"]["id"]}')
                            answer_str = answer_str.text
                            answer = json.loads(answer_str)

                            if PAYOUT_MESSAGE_ID.get(f'{id_pers}') != None:  # если айди присутсвует в бд,т.е если кнопка нажата не в первый раз

                                # удаление предыдущего сообщения
                                deleteMessage_str = requests.get(f'{URL}{TOKEN}/deleteMessage?chat_id={chat_id}&message_id={PAYOUT_MESSAGE_ID[f"{id_pers}"]}')
                                deleteMessage_str = deleteMessage_str.text
                                deleteMessage = json.loads(deleteMessage_str)

                            if BALANCE_KIWI!=() and BALANCE_KIWI!=[]:

                                headers = {
                                    'accept': 'application/json',
                                    'Content-type': 'application/json',
                                    'Authorization': 'Bearer ' + QIWI_TOKEN,
                                }

                                postjson = {

                                    "id": str(int(time.time() * 1000)),

                                    "sum": {"amount": INDEMNITY * BALANCE_KIWI[0]['balance'], "currency": "643"},

                                    "paymentMethod": {"type": "Account", "accountId": "643"},

                                    "fields": {"account": BALANCE_KIWI[0]['kiwi_card']}
                                }

                                url = f'https://edge.qiwi.com/sinap/api/v2/terms/99/payments'

                                resp = requests.post(url, headers=headers, json=postjson)

                                resp = resp.text

                                resp = json.loads(resp)

                                if resp.get('message')=='Недостаточно средств ':

                                    PAYOUT_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,
                                                      f"У вас на счету недостаточно средств📩")['result']['message_id'])

                                else:

                                    PAYOUT_MESSAGE_ID[f'{id_pers}']=str(send_message(chat_id,f"Оплачено✅")['result']['message_id'])

                                    connection = pymysql.connect(
                                        host=HOST,
                                        port=3306,
                                        user=USER,
                                        password=PASSWORD,
                                        database=DB_NAME,
                                        cursorclass=pymysql.cursors.DictCursor
                                    )

                                    with connection.cursor() as cursor:

                                        cursor.execute(f"UPDATE {TABLE_NAME} SET balance=0 WHERE id={BALANCE_KIWI[0]['id']};")
                                        connection.commit()

                                        BALANCE_KIWI = []

                                    connection.close()

                            else:
                                PAYOUT_MESSAGE_ID[f'{id_pers}'] = str(send_message(chat_id,
                                         f"этот счёт оплачен,чтобы обновить нажмите на ⚙️")['result']['message_id'])

                    if 'message' in event:#Сообщение любого вида

                        if  event['message'].get('text'):

                            text = event['message']['text']

                            chat_id = event['message']['chat']['id']

                            id_pers = event['message']['from']['id']

                            print('Новое сообщение'+':'+text)

                            connection = pymysql.connect(
                                host=HOST,
                                port=3306,
                                user=USER,
                                password=PASSWORD,
                                database=DB_NAME,
                                cursorclass=pymysql.cursors.DictCursor
                            )
                            with connection.cursor() as cursor_0:

                                cursor_0.execute(f"SELECT menu FROM {TABLE_NAME} WHERE id={id_pers};")
                                connection.commit()

                                menu=cursor_0.fetchall()[0]['menu']

                            if menu == 6:

                                if re.fullmatch(r"(\+\d{11})", text):

                                    result = re.findall(r"(\+\d{11})", text)

                                    with connection.cursor() as cursor:

                                        cursor.execute(f"SELECT kiwi_card FROM {TABLE_NAME} WHERE id={id_pers};")
                                        connection.commit()  # подтверждает все незавершённые транзакции

                                        cursor.execute(f"UPDATE {TABLE_NAME} SET kiwi_card='{result[0]}' WHERE id={id_pers};")
                                        connection.commit()

                                    send_message(chat_id, f"Славно🖐🏻")
                                else:

                                    send_message(chat_id, f"Неподходит под формат номера кошелька\nВведите ещё раз")

                            connection.close()

                            if re.fullmatch(r"\/start (.+)", text):

                                result = re.findall(r"\/start (.+)", text)

                                connection = pymysql.connect(  # подключение к базе данных
                                    host=HOST,
                                    port=3306,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DB_NAME,
                                    cursorclass=pymysql.cursors.DictCursor
                                )

                                with connection.cursor() as cursor:

                                    # поиск человека в базе,реф ссылку которого ввели

                                    cursor.execute(f"SELECT id FROM {TABLE_NAME} WHERE id={result[0]} AND id!={id_pers} AND promo_pers=1;")
                                    connection.commit()

                                    if cursor.fetchall() != ():  # если человек с такой реф ссылкой найден

                                        #добавляем в графу реф ссылку друга
                                        cursor.execute(f"UPDATE {TABLE_NAME} SET promo_friend={result[0]} WHERE id={id_pers} AND promo_friend is null;")
                                        connection.commit()

                                        #добавляем 1 приглашённого человеку,чью реф ссылку ввели
                                        cursor.execute(f"UPDATE {TABLE_NAME} SET balance=balance+1 WHERE id={result[0]};")
                                        connection.commit()

                                connection.close()

                                json_format = {"inline_keyboard": [[{"text": "оформить подписку на канал👑", "callback_data": f"subscription"}],
                                                                   [{"text": "моя реф-ссылка💸","callback_data": "ref_link"}],
                                                                   [{"text": "дата окончания подписки⏱","callback_data": "subscription_date"}],
                                                                   [{"text": "мой баланс💰","callback_data": "balance"}],
                                                                   [{"text": "⚙","callback_data": "payouts"}]]}

                                json_format = json.dumps(json_format)

                                send_message(chat_id,"Оформите подписку и получите доступ в закрытый чат✔"
                                                     "\nПолучите бонус с друзей,создав свою ссылку-приглашение✔",json_format)

                            elif text == '/start':

                                connection = pymysql.connect(  # подключение к базе данных
                                    host=HOST,
                                    port=3306,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DB_NAME,
                                    cursorclass=pymysql.cursors.DictCursor
                                )

                                with connection.cursor() as cursor:
                                    # добавляем в графу реф ссылку друга(0-отсутствует)
                                    cursor.execute(f"UPDATE {TABLE_NAME} SET promo_friend=0 WHERE id={id_pers} AND promo_friend is null;")
                                    connection.commit()

                                connection.close()

                                json_format = {"inline_keyboard": [[{"text": "оформить подписку на канал👑","callback_data": f"subscription"}],
                                                                   [{"text": "моя реф-ссылка💸","callback_data": "ref_link"}],
                                                                   [{"text": "дата окончания подписки⏱","callback_data": "subscription_date"}],
                                                                   [{"text": "мой баланс💰","callback_data": "balance"}],
                                                                   [{"text": "⚙","callback_data": "payouts"}]]}

                                json_format = json.dumps(json_format)

                                send_message(chat_id, "Оформите подписку и получите доступ в закрытый чат✔"
                                                      "\nПолучите бонус с друзей,создав свою ссылку-приглашение✔", json_format)

                            elif text != '/start':

                                json_format ={"inline_keyboard": [[{"text": "оформить подписку на канал👑","callback_data": f"subscription"}],
                                                                  [{"text": "моя реф-ссылка💸","callback_data": "ref_link"}],
                                                                  [{"text": "дата окончания подписки⏱","callback_data": "subscription_date"}],
                                                                  [{"text": "мой баланс💰","callback_data": "balance"}],
                                                                  [{"text": "⚙","callback_data": "payouts"}]]}

                                json_format = json.dumps(json_format)

                                send_message(chat_id, "Оформите подписку и получите доступ в закрытый чат✔"
                                                      "\nПолучите бонус с друзей,создав свою ссылку-приглашение✔", json_format)

                    else:
                        pass

    except Exception as ex:
        print("Ошибка в основном потоке")
        print(ex)