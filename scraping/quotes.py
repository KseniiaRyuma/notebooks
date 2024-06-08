
import scrapy
from scrapy import Request
from scrapy.http import HtmlResponse
from scrapy.spiders import SitemapSpider

from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from langchain_text_splitters import RecursiveCharacterTextSplitter
import hashlib
import sys
import cohere
from pinecone import Pinecone
from bs4 import BeautifulSoup


API_KEY = ""  # fill in your Cohere API key here
co = cohere.Client(API_KEY)

pc = Pinecone(api_key="")
index = pc.Index("juniper")

def embed_and_index(chunks, url, text):
    # Function to split the input texts into batches
    def batch_texts(texts, batch_size):
        for i in range(0, len(texts), batch_size):
            yield texts[i:i + batch_size]

    # Split texts into batches of 96 (or another size <= 96 if desired)
    text_batches = batch_texts(chunks, 96)
    embeddings = []
    # sparse_vectors = []
    for batch in text_batches:
        print(batch)
        # Generate embeddings for each batch
        response = co.embed(
            texts=batch,
            model='embed-english-v3.0',
            input_type='search_document'
        )
        d_v = response.embeddings

        embeddings.extend(d_v)


    def generate_id(text):
        hash_object = hashlib.sha256(text.encode())  # Encode the text to bytes, then hash it
        hash_id = hash_object.hexdigest()  # Get the hexadecimal string representation of the hash
        return hash_id

    vectors = []
    for emb, chunk in zip(embeddings, chunks):
        kb = 1024
        max_size_kb = 30
        text_to_send = text
        if sys.getsizeof(text) > max_size_kb * kb:
            header_ending = 'Content: '
            chunk_without_header = chunk[chunk.index(header_ending) + len(header_ending):]
            chunk_start = text.index(chunk_without_header)
            start_index = chunk_start - (max_size_kb//2) * kb
            end_index = chunk_start + (max_size_kb//2) * kb
            shift = max(0, -start_index)
            start_index += shift
            end_index += shift
            text_to_send = text[start_index: end_index]

        metadata = {'url': url, 'all_text': text_to_send, 'chunk': chunk}

        vectors.append(
            {
                "id": str(generate_id(chunk)),
                "values": emb,
                "metadata": metadata
            }
        )

    index.upsert(vectors=vectors)


class QuotesSpider(SitemapSpider):
    name = 'quotes'

    sitemap_urls = [
        'https://www.mist.com/site_documentation-sitemap.xml',
        'https://www.juniper.net/documentation/sitemap/sitemap.xml'
    ]
    sitemap_rules = [
        ('/us/en/', "parse"),
        ('mist.com', "parse")
    ]
    failed_urls = []

    def parse(self, response):
        parse_func = self.parse_mist if 'mist.com' in response.url else self.parse_documentations
        yield SeleniumRequest(
            dont_filter=True,
            url=response.url,
            callback=parse_func,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.TAG_NAME, 'body'))
        )

    def extract_content(self, response, title_selectors, body_selector):
        try:
            soup = BeautifulSoup(response.body)
            text = soup.get_text().strip()

            url = response.url
            print(url)
            title = 'Title not found'
            for title_selector in title_selectors:
                title_element = soup.find(class_=title_selector)
                if title_element:
                    title = title_element.text.strip()
                    break

            detail = soup.find(class_=body_selector)
            if detail:
                detail = detail.text.strip()
            else:
                detail = text

            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=512,
                chunk_overlap=50,
                length_function=len,
                is_separator_regex=False,
            )

            # Split the text into chunks with some overlap
            chunks_ = text_splitter.create_documents([detail])
            chunks = ['Title: {}\n'.format(title) + "\n\n Content: " + c.page_content for c in chunks_]

            embed_and_index(chunks, url, text)
        except:
            print(f'URL FAILED: {response.url}')
            self.failed_urls.append(response.url)
    def parse_documentations(self, response):
        self.extract_content(response, ['topictitle1'], 'topicBody')

    def parse_mist(self, response):
        self.extract_content(response, ['content-title', 'entry-title'],
                             'site-documentation__posts')

    def handle_spider_closed(self, reason):
        self.crawler.stats.set_value('failed_urls', ', '.join(self.failed_urls))
        print('\n'.join(self.failed_urls))




