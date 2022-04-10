from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, List, Tuple

import scrapy

from ..items import PostItem
from ..model import PostInfo, PushInfo


class PttdigitSpider(scrapy.Spider):
    name: str = 'pttDigit'
    allowed_domains: List[str] = ['pttdigit.com']
    start_urls: List[str] = ['http://pttdigit.com/']

    XPATH: Dict[str, Any] = {
        'hub': {
            'next_page': (
                '/html/body/div[@class="container"]'
                '/div[@class="row row-offcanvas row-offcanvas-right"]'
                '/div[@class="col-xs-12 col-sm-12"]'
                '/div[@class="row" and @style="padding:5px"]'
                '/div[@style="width:90%; margin: 30px auto 0; text-align:center;"]'
                '/ul[@class="pagination"]'
                '/li[11]/a/@href'
            ),
            'root': (
                '/html/body/div[@class="container"]'
                '/div[@class="row row-offcanvas row-offcanvas-right"]'
                '/div[@class="col-xs-12 col-sm-12"]'
                '/div[@class="row" and @style="padding:5px"]'
                '//div[@class="block-item"]'
            ),
            'url': (
                './span[@class="block-item-title"]'
                '/a/@href'
            ),
            'push': (
                './span[@class="amp-thrlst-thumb h1 f2"]'
                '/text()'
            ),
        },
        'post': {
            'root': (
                '/html/body/div[@class="container"]'
                '/div[@class="row row-offcanvas row-offcanvas-right"]'
                '/div[@class="col-xs-12 col-sm-8"]'
                '/div[@class="row" and @style="padding:5px;"]'
            ),
            'author': (
                './div[1]/div[@class="thread-head"]'
                '/div[2]/span[@class="caption"]'
                '/text()'
            ),
            'title': (
                './div[1]/div[@class="thread-head"]'
                '/h1[@class="thread-title"]'
                '/text()'
            ),
            'datetime': (
                './div[1]/div[@class="thread-head"]'
                '/div[3]/span[2]'
                '/text()'
            ),
            'content': (
                './div[3]/div[@id="main-content"]'
                './/text()[normalize-space()]'
            ),
        },
        'push': {
            'root': (
                '/html/body/div[@class="container"]'
                '/div[@claass="row row-offcanvas row-offcanvas-right"]'
                '/div[@class="col-xs-12 col-sm-8"]'
                '/div[3]/div[3]/div[@id="main-content"]'
                '//div[@class="push"]'
            ),
            'push_tag': './span[2]/text()',
            'pusher': './span[3]/text()',
            'push_content': './span[4]/text()',
            'push_datetime': './span[5]/text()',
        }
    }

    raise ValueError('stop')

    def parse(self, response: scrapy.http.Response):
        # get all posts infos.
        post_infos: List[PostInfo] = self.parse_hub(response=response)

        # extract all info from post.
        for post_info in post_infos:

            yield scrapy.Request(
                url=post_info.url,
                callback=self.parse_post,
                cb_kwargs={'post_info': post_info},
            )

        # crawle previous page.
        next_page_url = response.xpath(
            self.XPATH['hub']['next_page']).get()

        # parse next page.
        if next_page_url:
            self.log(f"crawle next page url: {next_page_url}", 20)
            yield response.follow(next_page_url, callback=self.parse)

    def parse_hub(self, response: scrapy.http.Response) -> List[PostInfo]:

        # get all posts selector in hub.
        posts: List[scrapy.Selector] = response.xpath(
            self.XPATH['hub']['root']
        )

        post_infos = defaultdict(list)

        for post in posts:
            # handle the schema value for `url`.
            target = 'url'
            xpath = self.XPATH['hub'][target]
            extracted = post.xpath(xpath).get()
            post_infos[target].append(response.urljoin(extracted))

            # handle the None value for `push` value.
            target = 'push'
            xpath = self.XPATH['hub'][target]
            extracted = post.xpath(xpath).get() or '0'
            post_infos[target].append(extracted)

        self.log(f"prase hub [{post_infos}]: {response.url}", 30)

        return PostInfo.from_dict(post_infos)

    def parse_post(self, response: scrapy.http.Response, post_info: PostInfo):

        # anker to post.
        post: scrapy.Selector = response.xpath(self.XPATH['post']['root'])

        # get datetime and content.
        post_info.update(
            self.xpath_parse(post, self.XPATH['post']))

        # get all pushs.
        pushes = self.parse_pushs(response)
        pushes = [push.to_dict() for push in pushes]
        post_info.pushes = pushes

        self.log(f"prase post [{len(post_info.pushes)}]: {response.url}", 30)

        yield PostItem(**post_info.to_dict())

    def parse_pushs(self, response: scrapy.http.Response) -> List[PushInfo]:
        # anker to push.
        pushes = response.xpath(self.XPATH['push']['root'])

        # get all push info and instant to PushInfos.
        return PushInfo.from_dict(self.xpath_parse(pushes, self.XPATH['push']))

    @staticmethod
    def xpath_parse(root: List[scrapy.Selector], xpath_dict: Dict[str, Any], *skip_targets) -> Dict[str, List[str]]:
        skip_targets = *skip_targets, 'root'
        infos = {}

        for target, xpath in xpath_dict.items():
            # skip specific targets.
            if target in skip_targets:
                continue

            # assert the xpath is in string type.
            assert isinstance(xpath, str), \
                TypeError(f'xpath_parse: xpath type error: {xpath}')

            infos.update({target: root.xpath(xpath).extract()})

        return infos
