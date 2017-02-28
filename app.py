import mysql.connector as mariadb
from SentimentProvider import AlchemyProvider, SemantriaProvider
import time

# Uncomment when config file is present
from config import semantria_key, semantria_secret, german_conf_twitter_active, german_conf, db_host, db_name, db_user, \
    db_password, db_port


def error_handling(statuscode, message):
    print message


# Function to update the Db
def update_db(sentiment = True, keywords = True):
    print "Updating Db entries..."
    print "Connection to DB."
    try:
        # connect to db
        mariadb_connection = mariadb.connect(host=db_host, port=db_port, user=db_user, password=db_password,
                                             database=db_name)

        cursor = mariadb_connection.cursor(buffered=True)
        print "Execute Select Statement"

        # providers = [#SemantriaProvider()]  # , AlchemyProvider()]
        providers = [AlchemyProvider()]

        for provider in providers:
            flag = True
            while flag:
                if sentiment is True:
                    update_sentiment_for_comments(provider, cursor)
                if keywords is True:
                    update_keywords_for_comments(provider,cursor)
                mariadb_connection.commit()

        mariadb_connection.close()

    except mariadb.Error as error:
        print "Error: {}".format(error)
    return


def update_keywords_for_comments(provider, cursor):
    # get data
    query_stmt = "SELECT id,text FROM post p LEFT JOIN post_has_keyword ON p.id = post_has_keyword.post_id WHERE p.text IS NOT NULL AND length(trim(text)) != 0 AND post_has_keyword.keyword_id is NULL LIMIT 100"
    print(query_stmt)
    cursor.execute(query_stmt)

    if cursor.rowcount == 0:
        print("No values to be updated. Terminating update process.")
        flag = False
    else:
        print("Rows to process: " + str(cursor.rowcount))

        # prepare data
        input_texts = []
        for id, text in cursor:
            input_texts.append({"id": id, "text": text})

        # parse for sentiment
        docs = provider.parse_keywords(input_texts, 'German')

        if docs is not None:
            print(len(docs))
            # update database
            for doc in docs:

                for i in range(len(doc.keywords())):
                    # find wether keyword already exists
                    keyword = doc.keywords()[i]
                    relevance = doc.relevance()[i]
                    keyword_id = None
                    find_stmt = "select id from keyword where keyword.keyword = '" + keyword + "'"

                    while(keyword_id is None):
                        cursor.execute(find_stmt)
                        if cursor.rowcount == 0:
                            keyword_stmt = 'INSERT INTO keyword(`keyword`) VALUES("' + keyword + '")'
                            print(keyword_stmt)
                            cursor.execute(keyword_stmt)
                        else:
                            result = cursor.fetchall()
                            keyword_id = result[0][0]

                    post_has_keyword_statement = 'INSERT INTO post_has_keyword(`keywordProvider_id`, `post_id`, `keyword_id`, `relevance`) VALUES("' + str(
                        provider.provider_id()) + '", "' + str(
                        doc.postid()) + '", "' + str(
                        keyword_id) + '", "' + str(
                        relevance) + '")'
                    print(post_has_keyword_statement)
                    cursor.execute(post_has_keyword_statement)

            print "Updated " + str(len(docs)) + " entries in the database."

def update_sentiment_for_comments(provider, cursor):
    # get data
    query_stmt = "SELECT id, text FROM comment c WHERE id NOT IN (SELECT comment_id FROM sentiment s WHERE sentimentProvider_id = " \
                 + str(provider.provider_id()) + " AND s.comment_id = c.id) LIMIT 100"
    print(query_stmt)
    cursor.execute(query_stmt)

    if cursor.rowcount == 0:
        print("No values to be updated. Terminating update process.")
        flag = False
    else:
        print("Rows to process: " + str(cursor.rowcount))

        # prepare data
        input_texts = []
        for id, text in cursor:
            input_texts.append({"id": id, "text": text})

        # parse for sentiment
        docs = provider.parse_sentiment(input_texts, 'German')

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


while True:
    update_db(False, True)
    time.sleep(6000)
