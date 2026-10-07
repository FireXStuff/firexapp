import unittest
from collections import OrderedDict
from html.parser import HTMLParser

import pytest

from firexkit.firexkit_common import get_link, sec2hms


class SimpleHtmlParser(HTMLParser):
    def __init__(self, html_str):
        super().__init__()
        self.start_tag, self.start_tag_attrs, self.data = None, None, None
        self.feed(html_str)

    def handle_starttag(self, tag, attrs):
        self.start_tag = tag
        self.start_tag_attrs = OrderedDict(attrs)

    def handle_data(self, data):
        self.data = data


class HtmlTemplateTests(unittest.TestCase):
    def test_simple_link(self):
        url = "http://some.com/path"
        text = "content<b> with markup</b>"
        link = get_link(url, text=text)

        parser = SimpleHtmlParser(link)
        self.assertEqual(parser.data, text)
        self.assertEqual(parser.start_tag_attrs["href"], url)

    def test_custom_attrs_link(self):
        url = "http://some.com/path"
        text = "content<b> with markup</b>"
        link = get_link(url, text=text, attrs={"a": "b"})

        parser = SimpleHtmlParser(link)
        self.assertEqual(parser.data, text)
        self.assertEqual(parser.start_tag_attrs["href"], url)
        self.assertEqual(parser.start_tag_attrs["a"], "b")


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (0, "0s"),
        (9, "9s"),
        (60, "1m0s"),
        (90, "1m30s"),
        (60 * 60, "1h0s"),
        (3723, "1h2m3s"),
        # The run time limit messages pass floats straight through.
        (3723.9, "1h2m3s"),
        # A run that is already past its deadline has negative time remaining; the
        # sign belongs on the duration as a whole, not on each unit within it.
        (-90, "-1m30s"),
    ],
)
def test_sec2hms(seconds, expected):
    assert sec2hms(seconds) == expected
