from html.parser import HTMLParser
from collections import OrderedDict
import unittest

from firexkit.firexkit_common import get_link


class SimpleHtmlParser(HTMLParser):

    def __init__(self, html_str):
        super(SimpleHtmlParser, self).__init__()
        self.start_tag, self.start_tag_attrs, self.data = None, None, None
        self.feed(html_str)

    def handle_starttag(self, tag, attrs):
        self.start_tag = tag
        self.start_tag_attrs = OrderedDict(attrs)

    def handle_data(self, data):
        self.data = data


class HtmlTemplateTests(unittest.TestCase):
    def test_simple_link(self):
        url = 'http://some.com/path'
        text = 'content<b> with markup</b>'
        link = get_link(url, text=text)
        print(link)

        parser = SimpleHtmlParser(link)
        print(parser.start_tag_attrs)
        self.assertEqual(parser.data, text)
        self.assertEqual(parser.start_tag_attrs['href'], url)

    def test_custom_attrs_link(self):
        url = 'http://some.com/path'
        text = 'content<b> with markup</b>'
        link = get_link(url, text=text, attrs={'a': 'b'})

        parser = SimpleHtmlParser(link)
        self.assertEqual(parser.data, text)
        self.assertEqual(parser.start_tag_attrs['href'], url)
        self.assertEqual(parser.start_tag_attrs['a'], 'b')

