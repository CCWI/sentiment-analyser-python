import mysql.connector as mariadb
from SentimentProvider import AlchemyProvider, SemantriaProvider
import time

# Uncomment when config file is present
from config import semantria_key, semantria_secret, german_conf_twitter_active, german_conf, db_host, db_name, db_user, \
    db_password, db_port


def error_handling(statuscode, message):
    print message


# Function to update the Db
def update_db(sentiment = True, keywords = True, picture_keywords = True):
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
                    update_keywords_for_comments(provider, cursor)
                if picture_keywords is True:
                    update_keywords_for_pictures(provider, cursor)
                mariadb_connection.commit()

        mariadb_connection.close()

    except mariadb.Error as error:
        print "Error: {}".format(error)
    return


def update_keywords_for_pictures(provider, cursor):
    provider_id = provider.provider_id()
    # get data
    query_stmt = "SELECT id, picture FROM post p LEFT JOIN post_has_class ON p.id = post_has_class.post_id" + \
                 " WHERE p.picture IS NOT NULL AND post_has_class.class_id is NULL LIMIT 5"
    print(query_stmt)
    cursor.execute(query_stmt)

    if cursor.rowcount == 0:
        print("No values to be updated. Terminating update process.")
        flag = False
    else:
        print("Rows to process: " + str(cursor.rowcount))

        # prepare data
        input_urls = []
        for id, picture in cursor:
            input_urls.append({"id": id, "picture": picture})

        # parse for keywords
        docs = provider.parse_picture_keywords(input_urls)

        if docs is not None:
            print(len(docs))
            # update database
            for doc in docs:
                post_id = doc.postid()

                if len(doc.classes()) == 0:
                    # insert dummy class
                    insert_class(cursor, '', post_id, provider_id, '')
                else:
                    for i in range(len(doc.classes())):
                        # find wether keyword already exists
                        current_class = doc.classes()[i]
                        score = doc.score()[i]
                        insert_class(cursor, current_class, post_id, provider_id, score)

            print "Updated " + str(len(docs)) + " entries in the database."


def update_keywords_for_comments(provider, cursor):
    provider_id = provider.provider_id()
    # get data
    query_stmt = "SELECT id,text FROM post p LEFT JOIN post_has_keyword ON p.id = post_has_keyword.post_id" + \
                 " WHERE p.text IS NOT NULL AND length(trim(text)) != 0 AND post_has_keyword.keyword_id is NULL LIMIT 100"
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

        # parse for keywords
        docs = provider.parse_keywords(input_texts, 'German')

        if docs is not None:
            print(len(docs))
            # update database
            for doc in docs:
                post_id = doc.postid()

                if len(doc.keywords()) == 0:
                    # insert dummy keyword
                    insert_keyword(cursor, '', post_id, provider_id, '')
                else:
                    for i in range(len(doc.keywords())):
                        # find wether keyword already exists
                        keyword = doc.keywords()[i]
                        relevance = doc.relevance()[i]
                        insert_keyword(cursor, keyword, post_id, provider_id, relevance)

            print "Updated " + str(len(docs)) + " entries in the database."


def insert_class(cursor, class_name, post_id, provider_id, score):
    class_id = None
    find_stmt = "SELECT id FROM class WHERE class.class = '" + class_name + "'"
    while class_id is None:
        cursor.execute(find_stmt)
        if cursor.rowcount == 0:
            keyword_stmt = 'INSERT INTO class(`class`) VALUES("' + class_name + '")'
            print(keyword_stmt)
            cursor.execute(keyword_stmt)
        else:
            result = cursor.fetchall()
            class_id = result[0][0]
    post_has_class_statement = 'INSERT INTO post_has_class(`sentimentProvider_id`, `post_id`, `class_id`, `score`) VALUES("' + str(
        provider_id) + '", "' + str(
        post_id) + '", "' + str(
        class_id) + '", "' + str(
        score) + '")'
    print(post_has_class_statement)
    cursor.execute(post_has_class_statement)


def insert_keyword(cursor, keyword, post_id, provider_id, relevance):
    keyword_id = None
    find_stmt = "SELECT id FROM keyword WHERE keyword.keyword = '" + keyword + "'"
    while keyword_id is None:
        cursor.execute(find_stmt)
        if cursor.rowcount == 0:
            keyword_stmt = 'INSERT INTO keyword(`keyword`) VALUES("' + keyword + '")'
            print(keyword_stmt)
            cursor.execute(keyword_stmt)
        else:
            result = cursor.fetchall()
            keyword_id = result[0][0]
    post_has_keyword_statement = 'INSERT INTO post_has_keyword(`sentimentProvider_id`, `post_id`, `keyword_id`, `relevance`) VALUES("' + str(
        provider_id) + '", "' + str(
        post_id) + '", "' + str(
        keyword_id) + '", "' + str(
        relevance) + '")'
    print(post_has_keyword_statement)
    cursor.execute(post_has_keyword_statement)


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
    update_db(False, False, True)
    time.sleep(6000)
