import mysql.connector as mariadb
from SentimentProvider import AlchemyProvider, SemantriaProvider
import time

# Uncomment when config file is present
from config import semantria_key, semantria_secret, german_conf_twitter_active, german_conf, db_host, db_name, db_user, db_password, db_port

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

        providers = []
        #providers.append(SemantriaProvider())
        providers.append(AlchemyProvider())

        for provider in providers:
            flag = True
            while flag:
                # get data
                query_stmt = "SELECT id, text FROM comment c WHERE id NOT IN (SELECT comment_id FROM sentiment s WHERE sentimentProvider_id = " \
                             + str(provider.provider_id()) + " AND s.comment_id = c.id) LIMIT 100"
                print(query_stmt)
                cursor.execute(query_stmt)
                print cursor.rowcount
                if cursor.rowcount == 0:
                    print("No values to be updated. Terminating update process.")
                    flag = False
                else:
                    # prepare data
                    input_texts = []
                    for id, text in cursor:
                        input_texts.append({"id": id, "text": text})

                    print(len(input_texts))

                    # parse for sentiment
                    docs = provider.parse(input_texts, 'German')

                    if docs is not None:
                        print(len(docs))
                        # update database
                        for doc in docs:
                            stmt = 'INSERT INTO sentiment(`sentimentProvider_id`, `comment_id`, `sentiment`, `mixed`) VALUES("' + str(
                                provider.provider_id()) + '", "' + str(
                                doc.id()) + '", "' + str(
                                doc.sentiment_score()) + '", "' + str(
                                doc.mixed()) + '")'
                            print(stmt)
                            cursor.execute(stmt)

                        print "Updated " + str(len(docs)) + " entries in the database."
                        mariadb_connection.commit()


        mariadb_connection.close()
    except mariadb.Error as error:
        print "Error: {}".format(error)
        return

while True:
    update_db()
    time.sleep(6000)
