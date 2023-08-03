import asyncio
from typing import List
import httpx
from loguru import logger as log
from parsel import Selector

def find_urls(resp: httpx.Response, xpath: str) -> set:
    """find crawlable urls in a response from an xpath"""
    found = set()
    urls = Selector(text=resp.text).xpath(xpath).getall()
    for url in urls:
        url = httpx.URL(resp.url).join(url.split("#")[0])
        if url.host != resp.url.host:
            log.debug(f"skipping url of a different hostname: {url.host}")
            continue
        found.add(str(url))
    return found

async def crawl(url, follow_xpath: str, session: httpx.AsyncClient, max_depth=10) -> List[httpx.Response]:
    """crawl source with provided follow rules"""
    urls_seen = set()
    urls_to_crawl = [url]
    all_responses = []
    depth = 0
    while urls_to_crawl:
        # first we want to protect ourselfes from accidental infinite crawl loops
        if depth > max_depth:
            log.error(f"max depth reached with {len(urls_to_crawl)} urls left in the crawl queue")
            break
        log.info(f"scraping: {len(urls_to_crawl)} urls")
        responses = await asyncio.gather(*[session.get(url) for url in urls_to_crawl])
        found_urls = set()
        for resp in responses:
            all_responses.append(resp)
            found_urls = found_urls.union(find_urls(resp, xpath=follow_xpath))
        # find more urls to crawl that we haven't visited before:
        urls_to_crawl = found_urls.difference(urls_seen)
        urls_seen = urls_seen.union(found_urls)
        depth += 1
    log.info(f"found {len(all_responses)} responses")
    return all_responses

async def run():
    limits = httpx.Limits(max_connections=3)
    headers = {"User-Agent": "Tim try to index ezp docs"}
    async with httpx.AsyncClient(limits=limits, headers=headers) as session:
        responses = await crawl(
            #our starting point url
            url="https://ezpublishdoc.mugo.ca/eZ-Publish/Technical-manual/4.x.html",
            follow_xpath="//li[contains(@class, 'topchapter')]//a/@href",
            session=session,
        )
if __name__ == "__main__":
    asyncio.run(run())