import json
import re
import time
import uuid

import semantria
from watson_developer_cloud import AlchemyLanguageV1
from watson_developer_cloud import VisualRecognitionV3
from watson_developer_cloud import watson_developer_cloud_service

from SatException import SatException
from config import semantria_key, semantria_secret, alchemy_key, visual_recognition_key, visual_recognition_version, german_conf_twitter_active, german_conf


class SentimentProvider(object):
    def __init__(self, name, provider_id):
        object.__init__(self)
        self._name = name
        self._provider_id = provider_id

    def name(self):
        return self._name

    def setname(self, name):
        self._name = name

    def provider_id(self):
        return self._provider_id

    def setprovider_id(self, provider_id):
        self._provider_id = provider_id

    def parse_sentiment(self, input_texts, expected_lang):
        # Use Configuration for specific language
        print("Parsing Sentiment with provider " + self._name)

    def parse_keywords(self, input_texts, expected_lang):
        print("Parsing Keywords with provider " + self._name)

    def parse_picture_keywords(self, input_urls):
        print("Parsing Keywords from picture, with provider " + self._name)

class SemantriaProvider(SentimentProvider):
    def __init__(self):
        SentimentProvider.__init__(self, 'Semantria', 1)

    # Function to parse input text to a maximum of 100 pieces in a certain language (only German supported atm).
    def parse_sentiment(self, input_texts, expected_lang):
        SentimentProvider.parse_sentiment(self, input_texts, expected_lang)

        if len(input_texts) > 100:
            raise SatException("Too many inputs. Input documents limited at 100 per API call!")

        # Parse messages from json file
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
        session = semantria.Session(semantria_key, semantria_secret, serializer, use_compression=True)

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

        results = results_more140 + results_less140
        responses = []
        for result in results:
            responses.append(SentimentResponse(result['id'], result['sentiment_score'], None))
        return responses

    def parse_keywords(self, input_texts, expected_lang):
        print("Keyword parsing not implemeted yet for " + self.name())


class AlchemyProvider(SentimentProvider):
    def __init__(self):
        SentimentProvider.__init__(self, 'Alchemy', 2)
        self._alchemy_language = AlchemyLanguageV1(api_key=alchemy_key)
        self._visual_recognition = VisualRecognitionV3(version=visual_recognition_version,
                                                       api_key=visual_recognition_key)

    def parse_sentiment(self, input_texts, expected_lang):
        SentimentProvider.parse_sentiment(self, input_texts, expected_lang)

        responses = []

        for comment in input_texts:
            comment_text = comment["text"]
            # print("Comment: " + comment_text)
            try:
                if comment_text is None or len(comment_text.strip()) == 0:
                    print("Skipping comment. Text is empty!")
                else:
                    result_sentiment = self._alchemy_language.sentiment(text=comment_text,
                                                                        language=expected_lang.lower())
                    print(json.dumps(result_sentiment, indent=2))
                    doc_sentiment = result_sentiment["docSentiment"]
                    sentiment_response = SentimentResponse(comment["id"],
                                                           doc_sentiment["score"] if "score" in doc_sentiment else 0,
                                                           doc_sentiment["mixed"] if 'mixed' in doc_sentiment else 0)
                    responses.append(sentiment_response)
            except watson_developer_cloud_service.WatsonException as e:
                print(str(e)) # + " Comment: " + comment_text)

        return responses

    def parse_keywords(self, input_texts, expected_lang):
        SentimentProvider.parse_keywords(self, input_texts, expected_lang)

        responses = []

        for post in input_texts:
            post_text = post["text"]
            try:
                if post_text is None or len(post_text.strip()) == 0:
                    print("Skipping comment. Text is empty!")
                else:
                    result_keywords = self._alchemy_language.keywords(text=post_text,
                                                                      language=expected_lang.lower())
                    print(json.dumps(result_keywords, indent=2))
                    if 'keywords' in result_keywords:
                        keywords_dict = result_keywords["keywords"]
                    else:
                        keywords_dict = {}
                    keywords_list = []
                    relevance_list = []
                    for keyword in keywords_dict:
                        if keyword["relevance"] >= 0.6:
                            keywords_list.append(keyword["text"])
                            relevance_list.append(keyword["relevance"])
                    keywords_response = KeywordResponse(post["id"], keywords_list, relevance_list)
                    responses.append(keywords_response)
            except watson_developer_cloud_service.WatsonException as e:
                print(str(e) + " Post: " + post_text)

        return responses

    def parse_picture_keywords(self, input_urls):
        SentimentProvider.parse_picture_keywords(self, input_urls)

        responses = []

        for post in input_urls:
            picture = post["picture"]
            try:
                if picture is None or len(picture.strip()) == 0:
                    print("Skipping URL. URL is empty!")
                else:
                    result = self._visual_recognition.classify(images_url=picture)
                    print(json.dumps(result, indent=2))
                    images_dict = result["images"][0]
                    if 'classifiers' in images_dict:
                        classifiers_dict = images_dict["classifiers"][0]
                        if 'classes' in classifiers_dict:
                            classes_dict = classifiers_dict["classes"]
                            classes_list = []
                            score_list = []
                            for class_item in classes_dict:
                                if class_item["score"] >= 0.6:
                                    classes_list.append(class_item["class"])
                                    score_list.append(class_item["score"])
                            keywords_response = PictureKeywordResponse(post["id"], classes_list, score_list)
                            responses.append(keywords_response)
            except watson_developer_cloud_service.WatsonException as e:
                print(str(e) + " URL: " + picture)

        return responses

class KeywordResponse(object):
    def __init__(self, postid, keywords, relevance):
        object.__init__(self)
        self._postid = postid
        self._keywords = keywords
        self._relevance = relevance

    def postid(self):
        return self._postid

    def setpostid(self, postid):
        self._postid = postid

    def keywords(self):
        return self._keywords

    def setkeywords(self, keywords):
        self._keywords = keywords

    def relevance(self):
        return self._relevance

    def setrelevance(self, relevance):
        self._relevance = relevance

class PictureKeywordResponse(object):
    def __init__(self, postid, classes, score):
        object.__init__(self)
        self._postid = postid
        self._classes = classes
        self._score = score

    def postid(self):
        return self._postid

    def setpostid(self, postid):
        self._postid = postid

    def classes(self):
        return self._classes

    def setclasses(self, classes):
        self._classes = classes

    def score(self):
        return self._score

    def setscore(self, score):
        self._score = score


class SentimentResponse(object):
    def __init__(self, id, sentiment_score, mixed):
        object.__init__(self)
        self._id = id
        self._sentiment_score = sentiment_score
        self._mixed = mixed

    def id(self):
        return self._id

    def setid(self, id):
        self._id = id

    def sentiment_score(self):
        return self._sentiment_score

    def setsentiment_score(self, sentiment_score):
        self._sentiment_score = sentiment_score

    def mixed(self):
        return self._mixed

    def setmixed(self, mixed):
        self._mixed = mixed
