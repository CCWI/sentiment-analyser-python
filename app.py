import os
import uuid
import semantria
import time
import atexit
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import mysql.connector as mariadb

# Uncomment when config file is present
# from config import key, secret, german_conf_twitter_active, german_conf

app = Flask(__name__)


class SatException(Exception):
    pass


def error_handling(statuscode, message):
    print message


# Function to update the Db
def update_db():
    print "Updating Db entries..."
    print "Connection to DB."
    # connect to db
    mariadb_connection = mariadb.connect(host="", port=3306, user='', password='', database='')
    cursor = mariadb_connection.cursor()
    print "Execute Select Statement"
    # get data
    cursor.execute("SELECT id,text FROM post WHERE sentiment IS NULL LIMIT 100")

    if cursor.rowcount == 0:
        print("No values updated.")
        mariadb_connection.close()
        return

    # prepare data
    input_texts = []
    for id, text in cursor:
        input_texts.append({"id": id, "text": text})

    # parse for sentiment
    docs = parse(input_texts, 'German')

    # update database
    try:
        for doc in docs:
            print doc["sentiment"]
            # cursor.execute("UPDATE post SET sentiment = " + doc["sentiment"] + "  WHERE id = " + doc["id"])
    except mariadb.Error as error:
        print("Error: {}".format(error))

    print "Updated " + str(len(docs)) + " entries in the database."


# Function to parse input text to a maximum of 100 pieces in a certain language (only German supported atm).
def parse(input_texts, expected_lang):
    if len(input_texts) > 100:
        raise SatException("Too many inputs. Input documents limited at 100 per API call!")

    # Parse messages from json file
    docs = []
    docs_less140 = []
    docs_more140 = []
    id_map = {}
    for comment in input_texts:
        id = str(uuid.uuid4()).replace("-", "")
        if id in id_map:
            raise SatException("No duplicate ids allowed.")
        id_map[id] = comment["id"]
        docs.append({"id": id, "text": comment["text"]})
        if len(comment["text"]) > 140:
            docs_more140.append({"id": id, "text": comment["text"]})
        else:
            docs_less140.append({"id": id, "text": comment["text"]})

    # Initalise JSON serialiser and create semantria Session
    serializer = semantria.JsonSerializer()
    session = semantria.Session(key, secret, serializer, use_compression=True)

    # Use Configuration for specific language
    lang_id_more140 = ""
    lang_id_less140 = ""
    print("Setting Language: " + expected_lang)

    if expected_lang != "German":
        raise SatException("Only 'German' is supported!")

    lang_id_less140 = german_conf_twitter_active
    lang_id_more140 = german_conf

    # Send messages as batch to semantria
    if len(docs_more140) > 0:
        session.queueBatch(docs_more140, lang_id_more140)
    if len(docs_less140) > 0:
        session.queueBatch(docs_less140, lang_id_less140)

    # Retrieve results
    length_more140 = len(docs_more140)
    results_more140 = []
    length_less140 = len(docs_less140)
    results_less140 = []

    while len(results_more140) < length_more140 or len(results_less140) < length_less140:
        print("Retrieving processed results...", "\r\n")
        time.sleep(2)
        # get processed documents
        status_more140 = session.getProcessedDocuments(lang_id_more140)
        print "Added " + str(len(status_more140)) + " entries to result_more140"
        results_more140.extend(status_more140)
        satus_less140 = session.getProcessedDocuments(lang_id_less140)
        print "Added " + str(len(satus_less140)) + " entries to result_less140"
        results_less140.extend(satus_less140)

    # Add sentiment value to all entries and remove those from list which arent in expected language
    for data in results_less140:
        doc = next((x for x in docs if x["id"] == data["id"]), None)
        if doc is None:
            break
        if data["language"] == expected_lang:
            doc["sentiment"] = data["sentiment_score"]
            doc["id"] = id_map[doc["id"]]
        else:
            docs.remove(doc)

    for data in results_more140:
        doc = next((x for x in docs if x["id"] == data["id"]), None)
        if doc is None:
            break
        if data["language"] == expected_lang:
            doc["sentiment"] = data["sentiment_score"]
            doc["id"] = id_map[doc["id"]]
        else:
            docs.remove(doc)

    return docs


update_db()
# scheduler = BackgroundScheduler()
# scheduler.start()
# scheduler.add_job(
#     func=update_db,
#     trigger=IntervalTrigger(minutes=1),
#     id='updatedb_job',
#     name='Looks for new entries in the database and adds their sentiment value',
#     replace_existing=True)
# # Shut down the scheduler when exiting the app
# atexit.register(lambda: scheduler.shutdown())


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
