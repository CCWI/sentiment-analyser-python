import os
import uuid
import semantria
import time
from flask import Flask, jsonify, request

from config import version, key, secret, german_conf_twitter_active, german_conf

app = Flask(__name__)


class SatException(Exception):
    pass


def error_handling(statuscode, message):
    return jsonify({'message': message}), statuscode


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
        results_more140.extend(status_more140)
        satus_less140 = session.getProcessedDocuments(lang_id_less140)
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


# Expects JSON to be formatted as:
# ATM only German is allowed as expected language
# {
#  	"messages": [{
#        "id": 1,
#  		"text": "Dies ist eine Nachricht"
#  	}, {
#        "id": 2,
#  		"text": "Und hier bin ich boese das ist scheisse"
#  	}],
#  	"expected_language": "German"
#  }
@app.route('/sat/api/v1.0/sentiment', methods=['POST'])
def get_sentiment():
    if not request.json or not 'messages' in request.json or not 'expected_language' in request.json:
        return error_handling(400, "No 'messages' or 'expected_language' key in json")
    try:
        sentiment_value = parse(request.json.get('messages'), request.json.get('expected_language'))
    except SatException as e:
        return error_handling(400, str(e))
    return jsonify(sentiment_value), 201


@app.route('/sat/api/version', methods=['GET'])
def get_version():
    return version


@app.route('/')
def hello_world():
    return 'Sentiment Analyser Root. \n Usage: \n - to get the version GET /sat/api/version \n to get the sentiment analysis POST /sat/api/sentiment'


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
