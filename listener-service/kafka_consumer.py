# Defined the kafka consumer
import json
from confluent_kafka import Consumer

# new imports
# to have access to the web service
import requests
# to have access to PostgreSQL database
import psycopg2
# To transforme the timestamps in datetieme
from datetime import datetime

# Conected to the Topic to can
c = Consumer({
    'bootstrap.servers': 'kafka',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})
c.subscribe(['events'])

# We need to use our local ip. This would be updated manually.
ip = '192.168.178.35'
# URLs to connected with the classification-service and character-service
url_saga = 'http://{}:5000/predict'.format(ip)
url_character = 'http://{}:5010/characters'.format(ip)

# To connect with the database
conexion = psycopg2.connect(
    host=ip,
    port=5432,
    database="seed",
    user="postgres",
    password="postgres")

# Execute the process

while True:
    msg = c.poll(1.0)

    if msg is None:
        # Added data
        print("No mesage\n\n")
        continue

    if msg.error():
        print("Consumer error: {}\n\n".format(msg.error()))
        continue

    # Recover the message
    data = json.loads(msg.value().decode('utf-8'))

    # Predict Saga
    resp = requests.post(url_saga, json={'text': data['text']})
    data['saga'] = resp.json()['label']

    # Access to the database
    cursor = conexion.cursor()

    # Insert data into Documents table.
    sql = "insert into documents(message_id, document_name, time, readers, saga) values (%s,%s,%s,%s,%s)"
    cursor.execute(sql, (str(data['id'])+str(data['time']), data['id'], data['time'], data['readers'], data['saga']))

    # Recover characters
    resp = requests.post(url_character, json={'text': data['text']})
    data['characters'] = resp.json()['characters']

    # Insert data into Charecters table. Only if we didn't save the document before.
    sql = "select document_name from characters where document_name = '{}'".format(data['id'])
    cursor.execute(sql)
    if cursor.rowcount == 0:
        for charac in data['characters']:
            sql = "insert into characters(message_id, document_name, charac) values (%s,%s,%s)"
            cursor.execute(sql, (str(data['id'])+str(data['time']), data['id'], charac))

    conexion.commit()


    # Write the data in the console.
    print('New Message received')
    print('Received message: {}'.format(data['id']))
    print('Time: {}'.format(data['time']))  # Timestamp format
    print('Day: {}'.format(datetime.fromtimestamp(data['time'])))  # Human-Readable format
    print('Readers: {}'.format(data['readers']))
    print('Saga: {}'.format(data['saga']))
    print('Characters: {}'.format(data['characters']))
    print('Text: {} ...\n\n'.format(data['text'][:100]))

conexion.close()
c.close()
