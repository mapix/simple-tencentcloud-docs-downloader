import os.path
from typing import List, Optional, Callable, Dict, Union
from urllib.parse import urlparse

from requests_html import HTMLSession
from tqdm import tqdm
from loguru import logger


class SiteDownloader:

    def __init__(self, bootstrap_urls: List[str],
                 download_filter: Callable,
                 followable_domains: List[str],
                 *,
                 followable_paths: Optional[Union[List[str], Dict]] = None,
                 save_direcory: Optional[str] = None,
                 max_depth: Optional[int] = 10,
                 max_download: Optional[int] = 1000,
                 ignore_query_params: bool = True,
                 ignore_fragment: bool = True,
                 ignore_paths: Optional[List[str]] = None,
                 cookies: Optional[Dict[str, str]] = None,
                 extra_query_params: Optional[Dict[str, str]] = None,
                 user_agent: Optional[str] = None):
        assert bootstrap_urls, "Bootstrap urls must be provided"
        assert download_filter, "Download filter must be provided"
        assert followable_domains, "Followable domains must be provided"
        self.bootstrap_urls = bootstrap_urls
        self.followable_domains = followable_domains
        self.save_direcory = save_direcory = save_direcory or "downloads"
        self.download_filter = download_filter
        self.max_depth = max_depth
        self.max_download = max_download
        self.ignore_query_params = ignore_query_params
        self.followable_paths = followable_paths
        self.ignore_fragment = ignore_fragment
        self.extra_query_params = extra_query_params
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        self.cookies = cookies or {} 
        self.session = HTMLSession()
        self.session.headers.update({"User-Agent": self.user_agent})
        self.session.cookies.update(self.cookies or {})
        self.ignore_paths = ignore_paths or []
        os.makedirs(self.save_direcory, exist_ok=True)

    def parse_links(self, url, links, download_links, depth=0):
        if self.max_depth and depth >= self.max_depth:
            logger.debug(f"Reached max depth of {self.max_depth}")
            return
        if self.max_download and len(download_links) >= self.max_download:
            logger.info(f"Reached max download limit of {self.max_download}")
            return

        logger.info(f"Processing {url}")
        domain = urlparse(url).netloc
        if domain not in self.followable_domains:
            return

        # inject extra query params if any, use urlparse to parse the url
        if self.extra_query_params:
            url = urlparse(url)._replace(
                query="&".join([f"{k}={v}" for k, v in self.extra_query_params.items()])
            ).geturl()

        response = self.session.get(url)
        if response.status_code != 200:
            return

        if 'text/html' in response.headers.get('Content-Type'):
            for link in response.html.absolute_links:
                if self.max_download and len(download_links) >= self.max_download:
                    logger.info(f"Reached max download limit of {self.max_download}")
                    break

                if self.ignore_query_params:
                    link = link.split("?")[0]
                if self.ignore_fragment:
                    link = link.split("#")[0]

                if link not in download_links and self.download_filter(link):
                    download_links.add(link)
                ur = urlparse(link)
                url_domain = ur.netloc
                url_path = ur.path

                skip = False
                follow = False

                if self.followable_paths is None:
                    follow = True
                else:
                    for followable_path in self.followable_paths:
                        if isinstance(followable_path, str) and url_path.startswith(followable_path):
                            follow = True
                            break
                        elif isinstance(followable_path, dict):
                            source_path_prefix = followable_path["source"]
                            target_path_prefix = followable_path["target"]
                            if url_path.startswith(source_path_prefix):
                                follow = True
                                link = link.replace(source_path_prefix, target_path_prefix)
                                break

                if self.ignore_paths is None:
                    skip = False
                else:
                    for ignore_path in self.ignore_paths:
                        if url_path.startswith(ignore_path):
                            skip = True
                            break
                if follow and not skip and link not in links and url_domain in self.followable_domains:
                    links.add(link)
                    self.parse_links(link, links, download_links, depth + 1)
        logger.info(f"Finished processing {url}")

    def download(self, url):
        logger.info(f"Downloading {url}")
        target_path = os.path.join(self.save_direcory, url.split("/")[-1])
        with open(target_path, "wb") as f:
            response = self.session.get(url, stream=True)
            if not response.ok:
                logger.error(f"Failed to download {url}")
                return
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded {url}")

    def run(self):
        print("1. Parse site urls")
        links = set()
        download_links = set()
        for url in tqdm(self.bootstrap_urls):
            self.parse_links(url, links, download_links)

        print("2. Download site content")
        for url in tqdm(download_links):
            self.download(url)


if __name__ == '__main__':
    tasks = [
        {
            "name": "云服务器",
            "bootstrap_urls": [
                "https://www.tencentcloud.com/zh/document/product/213",
            ],
            "followable_paths": [
                "/zh/document/product/213",
                {"source": "/document/product/213", "target": "/zh/document/product/213"},
            ],
            "followable_domains": ["www.tencentcloud.com", "intl.cloud.tencent.com"]
        },
        {
            "name": "GPU 服务器",
            "bootstrap_urls": [
                "https://www.tencentcloud.com/zh/document/product/560",
            ],
            "followable_paths": [
                "/zh/document/product/560",
                {"source": "/document/product/560", "target": "/zh/document/product/560"},
            ],
            "followable_domains": ["www.tencentcloud.com", "intl.cloud.tencent.com"]
        },
    ]

    for task in tasks:
        name = task["name"]
        bootstrap_urls = task["bootstrap_urls"]
        followable_paths = task.get("followable_paths")
        followable_domains = task["followable_domains"]
        save_direcory = os.path.join("downloads", name)
        download_filter = (lambda x: x.endswith(".pdf"))
        downloader = SiteDownloader(bootstrap_urls,
                                    download_filter,
                                    followable_domains,
                                    followable_paths=followable_paths,
                                    extra_query_params={"lang": "zh"},
                                    save_direcory=save_direcory,
                                    max_download=None)
        downloader.run()