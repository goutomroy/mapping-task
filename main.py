import logging
import concurrent.futures
import time
from threading import Lock

import requests
from requests import RequestException

from models import Article, TitleSection, TextSection, LeadSection, HeaderSection, ImageSection, MediaSection

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
article_dict = {}


def build_sections(article_response, media_response):
    sections = []

    for section in article_response["sections"]:
        if section["type"] == "title":
            sections.append(TitleSection(**section))
        elif section["type"] == "text":
            sections.append(TextSection(**section))
        elif section["type"] == "lead":
            sections.append(LeadSection(**section))
        elif section["type"] == "header":
            sections.append(HeaderSection(**section))

    for media in media_response:
        if media["type"] == "image":
            sections.append(ImageSection(**media))
        elif media["type"] == "media":
            sections.append(MediaSection(**media))

    return sections


def pull_article_media(article_id):

    url = f"https://mapping-test.fra1.digitaloceanspaces.com/data/media/{article_id}.json"

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        response = response.json()
    except RequestException as request_exception:
        logging.error(str(request_exception))
        return None

    return response


def pull_article_detail(lock: Lock, article_id):

    with lock:
        if article_id in article_dict:
            return

    url = f"https://mapping-test.fra1.digitaloceanspaces.com/data/articles/{article_id}.json"

    try:
        article_response = requests.get(url, timeout=5)
        article_response.raise_for_status()
        article_response = article_response.json()
        article_response["url"] = url
    except RequestException as request_exception:
        logging.error(str(request_exception))
        return None

    media_response = pull_article_media(article_id)

    if media_response:
        sections = build_sections(article_response, media_response)
        article_response["sections"] = sections
        article = Article(**article_response)
        article_dict[article.id] = article


def pull_partial_article_list():
    url = "https://mapping-test.fra1.digitaloceanspaces.com/data/list.json"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


def start_thread_pool(articles):

    with concurrent.futures.ThreadPoolExecutor() as executor:
        lock = Lock()
        for article in articles:
            executor.submit(pull_article_detail, lock, article["id"])


def start_pulling():
    while True:
        try:
            start_thread_pool(pull_partial_article_list())
            time.sleep(300)

        except RequestException as request_exception:
            logging.error(str(request_exception))

        except KeyboardInterrupt:
            logging.info("Stopped Explicitly.")
            for article in article_dict.values():
                print(article.dict())
            break


if __name__ == "__main__":
    article_dict.clear()
    start_pulling()
