import uuid

import re
import semantria
import time
import mysql.connector as mariadb

# Uncomment when config file is present
from config import key, secret, german_conf_twitter_active, german_conf, db_host, db_name, db_user, db_password, db_port


class SatException(Exception):
    pass


def error_handling(statuscode, message):
    print message


# Function to update the Db
def update_db():
    print "Updating Db entries..."
    print "Connection to DB."
    try:
        # connect to db
        mariadb_connection = mariadb.connect(host=db_host, port=db_port, user=db_user, password=db_password,
                                             database=db_name)

        cursor = mariadb_connection.cursor(buffered=True)
        print "Execute Select Statement"

        flag = True
        while flag:
            # get data
            cursor.execute("SELECT id,text FROM comment WHERE sentiment IS NULL LIMIT 100")
            print cursor.rowcount
            if cursor.rowcount == 0:
                print("No values to be updated. Terminating update process.")
                flag = False
            else:
                # prepare data
                input_texts = []
                for id, text in cursor:
                    input_texts.append({"id": id, "text": text})

                # parse for sentiment
                docs = parse(input_texts, 'German')

                # update database
                for doc in docs:
                    if 'sentiment_score' in doc:
                        stmt = "UPDATE comment SET sentiment = " + str(doc['sentiment_score']) + '  WHERE id = "' + str(
                            doc['id']) + '"'
                        cursor.execute(stmt)

                print "Updated " + str(len(docs)) + " entries in the database."
                mariadb_connection.commit()

        mariadb_connection.close()
    except mariadb.Error as error:
        print "Error: {}".format(error)
        return


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
        # generate unique id
        comment_id = str(uuid.uuid4()).replace("-", "")
        while comment_id in id_map:
            comment_id = str(uuid.uuid4()).replace("-", "")

        # Map id to orignal id of the comment
        id_map[comment_id] = comment["id"]

        # clean the text of any url
        comment["text"] = re.sub(r'https?://www\.[a-z\.0-9]+', '', comment["text"])
        comment["text"] = re.sub(r'www\.[a-z\.0-9]+', '', comment["text"])

        # add comment to list of overall comments and bigger/smalle 140 char
        if len(comment["text"]) > 140:
            docs_more140.append({"id": comment_id, "text": comment["text"]})
        else:
            docs_less140.append({"id": comment_id, "text": comment["text"]})

    # Initalise JSON serialiser and create semantria Session
    serializer = semantria.JsonSerializer()
    session = semantria.Session(key, secret, serializer, use_compression=True)

    # Use Configuration for specific language
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

    while (len(results_more140) < length_more140) or (len(results_less140) < length_less140):
        print("Retrieving processed results...", "\r\n")
        time.sleep(2)
        # get processed documents
        status_more140 = session.getProcessedDocuments(lang_id_more140)
        for data in status_more140:
            if data["id"] in id_map:
                data["id"] = id_map[data["id"]]
            else:
                status_more140.remove(data)
        print "Added " + str(len(status_more140)) + " entries to result_more140"
        results_more140.extend(status_more140)

        status_less140 = session.getProcessedDocuments(lang_id_less140)
        for data in status_less140:
            if data["id"] in id_map:
                data["id"] = id_map[data["id"]]
            else:
                status_less140.remove(data)
        print "Added " + str(len(status_less140)) + " entries to result_less140"
        results_less140.extend(status_less140)

    return results_more140 + results_less140


while True:
    update_db()
    time.sleep(6000)
