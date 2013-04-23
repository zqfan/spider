#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (C) 2013 Zhiqiang Fan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import re
import unittest
import logging
import Queue
import sqlite3
import pep8

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(TESTS_DIR)
SOURCE_DIR = os.path.join(ROOT_DIR, 'spider')
if not SOURCE_DIR in sys.path:
    sys.path.append(SOURCE_DIR)
from spider import Spider, SQLWorker


class PEP8Test(unittest.TestCase):
    def test_pep8(self):
        spider_pep8 = pep8.Checker(os.path.join(SOURCE_DIR, 'spider.py'))
        r = spider_pep8.check_all()
        test_spider_pep8 = pep8.Checker(__file__)
        r += test_spider_pep8.check_all()
        self.assertEqual(r, 0)


class SpiderTest(unittest.TestCase):
    def setUp(self):
        self.spider = Spider('http://www.google.com',
                             loglevel=1)

    def tearDown(self):
        self.spider.logger.handlers = []

    def test_is_valid_url_with_valid_url(self):
        r = self.spider.is_valid_url('http://www.google.com')
        self.assertTrue(r)
        r = self.spider.is_valid_url('https://www.google.com')
        self.assertTrue(r)

    def test_is_valid_url_with_invalid_url(self):
        r = self.spider.is_valid_url('www.google.com')
        self.assertFalse(r)
        r = self.spider.is_valid_url('google')
        self.assertFalse(r)

    def test_get_abs_url_with_valid_base(self):
        r = self.spider.get_abs_url(r'http://www.google.com/a/', 'b')
        self.assertEqual(r, r'http://www.google.com/a/b')
        r = self.spider.get_abs_url(r'http://www.google.com/a', 'b')
        self.assertEqual(r, r'http://www.google.com/b')
        r = self.spider.get_abs_url(r'https://www.google.com/a/c', '/b')
        self.assertEqual(r, r'https://www.google.com/b')
        r = self.spider.get_abs_url(r'http://www.google.com',
                                    r'http://www.facebook.com')
        self.assertEqual(r, r'http://www.facebook.com')
        r = self.spider.get_abs_url(None, r'http://www.google.com')
        self.assertEqual(r, r'http://www.google.com')

    def test_get_abs_url_with_invalid_base(self):
        r = self.spider.get_abs_url('http://', 'www.google.com')
        self.assertEqual(r, 'http://www.google.com')

    def test_get_abs_url_with_invalid_url(self):
        r = self.spider.get_abs_url(None, 'http://')
        self.assertEqual(r, 'http://')

    def test_get_all_links_with_valid_content(self):
        content = ''.join(('<html><body>',
                           '<a href="http://a/b/c"></a>',
                           '<a href="a/b/c"></a>',
                           '<a href="/a/b/c"></a>',
                           '<a href="a"></a>',
                           '<a href="#"></a>',
                           '<a href="javascript"></a>',
                           '</body></html>'))
        l1 = ['http://a/b/c', 'a/b/c', '/a/b/c', 'a', '#',
              'javascript']
        l2 = self.spider.get_all_links(content)
        self.assertEqual(l1, l2)

    def test_convert_to_unicode_gbk(self):
        s = u'测试'
        sgbk = s.encode('gbk')
        r = self.spider.convert_to_unicode(sgbk)
        self.assertEqual(r, s)

    def test_convert_to_unicode_gb2312(self):
        s = u'测试'
        sgb2312 = s.encode('gb2312')
        r = self.spider.convert_to_unicode(sgb2312)
        self.assertEqual(r, s)

    def test_convert_to_unicode_utf8(self):
        s = u'测试'
        sutf8 = s.encode('utf-8')
        r = self.spider.convert_to_unicode(sutf8)
        self.assertEqual(r, s)

    def test_convert_to_unicode_unknown(self):
        s = u'测试'
        sunknown = s.encode('utf-16')
        self.assertRaises(Exception, self.spider.convert_to_unicode,
                          sunknown)

    def test_filter_links_with_fragment(self):
        links = ['http://a#one', 'https://b/c/d#one#two']
        flinks = self.spider.filter_links(links)
        flinks.sort()
        links = ['http://a', 'https://b/c/d']
        links.sort()
        self.assertEqual(flinks, links)

    def test_filter_links_with_relative_url(self):
        links = ['www.google.com']
        flinks = self.spider.filter_links(links)
        links = ['http://www.google.com']
        self.assertEqual(flinks, links)

    def test_filter_links_with_duplicated_url(self):
        links = ['http://www.google.com', 'http://www.google.com']
        flinks = self.spider.filter_links(links)
        links = ['http://www.google.com']
        self.assertEqual(flinks, links)

    def test_filter_links_with_invalid_url(self):
        links = ['http://', 'https://']
        flinks = self.spider.filter_links(links)
        self.assertEqual(flinks, [])

    def test_get_logger(self):
        logger = self.spider.get_logger('/tmp/test.log', 5)
        logger.debug('logger test')
        log_msg_exist = False
        with open('/tmp/test.log') as f:
            lines = f.readlines()
            for line in lines:
                if line.strip().endswith('logger test'):
                    log_msg_exist = not log_msg_exist
        os.remove('/tmp/test.log')
        self.assertTrue(log_msg_exist)

    def test_verify_page_headers(self):
        headers = {'ETag': '123',
                   'Last-Modified': '456',
                   'Content-Type': 'text/html'}
        r = self.spider.verify_page_headers(headers)
        self.assertEqual(r.get('etag', ''), '123')
        self.assertEqual(r.get('lastmodified', ''), '456')

    def test_verify_page_headers_with_invalid_input(self):
        headers = {'content-type': 'msdoc'}
        self.assertRaises(Exception,
                          self.spider.verify_page_headers,
                          headers)

    def test_get_key_pattern_with_invalid_input(self):
        key = u'测试'.encode('utf-16')
        k = self.spider.get_key_pattern(key)
        self.assertEqual(k, None)
        key = None
        k = self.spider.get_key_pattern(key)
        self.assertEqual(k, None)

    def test_get_pattern(self):
        key = u'测试'.encode('gbk')
        k = self.spider.get_key_pattern(key)
        r = k.search(u'我是测试')
        self.assertNotEqual(r, None)


class SQLWorkerTest(unittest.TestCase):
    def setUp(self):
        self.queue = Queue.Queue()
        self.sql = SQLWorker(dbfile='/tmp/test.db',
                             in_queue=self.queue)
        self.conn = self.sql.get_sql_connection()

    def tearDown(self):
        self.conn.close()
        os.remove('/tmp/test.db')

    def test_get_sql_connection(self):
        conn = self.sql.get_sql_connection('/tmp/test2.db')
        self.assertNotEqual(conn, None)
        conn.close()
        os.remove('/tmp/test2.db')

    def test_dump_page(self):
        page = {'url': '1', 'content': '2', 'lastmodified': '3',
                'etag': '4', 'redirect': '5'}
        self.sql.get_sql_connection('/tmp/test.db')
        self.sql.dump_page(page)
        conn = sqlite3.connect('/tmp/test.db')
        curs = conn.cursor()
        curs.execute("select etag from pages where url='1';")
        a = curs.fetchone()
        curs.close()
        conn.close()
        self.assertEqual(a, (u'4',))


if __name__ == '__main__':
    unittest.main()
