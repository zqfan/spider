#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4

"""Simple Spider using coroutine to grab data from url."""

import os
import sys
import re
import copy
import urllib2
import sqlite3
import logging
import unittest
import gzip
import StringIO
import time
from Queue import Queue
from Queue import Empty
from threading import Thread
from threading import Timer

import gevent
from gevent import monkey

import argparse
import BeautifulSoup
import requests


#monkey.patch_all(dns=False)


class TreadPoolSlot(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.idle = True
        self.logger = logging.getLogger(__name__)
        self.start()

    def run(self):
        while True:
            # wait there
            func, args, kwargs = self.tasks.get()
            # oh, lalala, i'm a happy honeybee
            self.idle = False
            try:
                func(*args, **kwargs)
            except Exception, e:
                # log here
                self.logger.error('%s %s' % (e.__class__.__name__, e))
            # notify the queue this task is done
            self.tasks.task_done()
            self.idle = True


class TreadPool(object):
    """Simple thread pool class using Queue.Queue.

    Example:
    tp = TreadPool(10)
    tp.spawn(os.path.cwd())
    tp.spawn(os.path.dirname, '.')
    tp.joinall()
    """
    def __init__(self, num):
        """num: Max threads of this pool."""
        self.tasks = Queue(num)
        self.pool = []
        for i in range(num):
            self.pool.append(TreadPoolSlot(self.tasks))

    def spawn(self, func, *args, **kwargs):
        """Spawn the func, note that it will not be start immediately.

        When a func is spawned, it will wait for a slot to run, or it
        will be blocked.
        """
        self.tasks.put((func, args, kwargs))

    def joinall(self):
        """Wait all spawned tasks to finish."""
        self.tasks.join()

    def undone_tasks(self):
        """This is not thread safe method.

        Another thread may call the spawn method, which leading this
        method to a wrong result. Even in the same thread, the slot
        may turn into idle when this method is called, but this
        situation is just a slight unprecise. so you can call it for
        unprecise environment.
        """
        r = 0
        # for one thread using the thread pool
        # if all are idle, then undone task is 0
        # for multi-thread using the thread pool, it's wrong
        for thread in self.pool:
            if not thread.idle:
                r += 1
        if r:
            r += self.tasks.qsize()
        return r


class SQLWorker(Thread):
    def __init__(self,
                 dbfile='/tmp/sample.db',
                 in_queue=None,
                 logger=None):
        """SQLWorker init method.

        param: dbfile where to store database, only sqlite3 support
        param: in_queue where to get data
        param: logger logger object
        """
        Thread.__init__(self)
        self.daemon = True
        self.dbfile = dbfile
        self.in_queue = in_queue
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None

    def get_sql_connection(self, dbfile=None, need_table=True):
        """Get sql connection to dbfile.

        self.conn may change according to the dbfile
        If dbfile is none, self.dbfile is used
        If dbfile does not exist, it will create it init the database.
        """
        # already init the connection, just return it
        if self.conn and not dbfile:
            return self.conn
        db = dbfile or self.dbfile
        # it will create the dbfile if it doesn't exist
        self.conn = sqlite3.connect(db)
        if not need_table:
            return self.conn
        self.init_table()
        # close it to make the table available
        self.conn.close()
        # reconnect to get sqlite3 object
        self.conn = sqlite3.connect(db)
        self.logger.info('Connected to sql done')
        return self.conn

    def init_table(self, script=None):
        """Init sqlite3 database table."""
        conn = self.get_sql_connection(need_table=False)
        curs = conn.cursor()
        s = script or """
                      CREATE TABLE IF NOT EXISTS pages(
                      url,
                      content,
                      last_modified,
                      etag,
                      redirect);"""
        curs.executescript(s)
        curs.close()
        self.logger.info('database Initialization done')

    def dump_page(self, page):
        """Dump a page dict to sql."""
        conn = self.conn or self.get_sql_connection()
        curs = conn.cursor()
        self.logger.debug('dump %s' % page['url'])
        # avoid sql injection
        curs.execute('INSERT INTO pages VALUES (?,?,?,?,?);',
                     (page['url'],
                      page['content'],
                      page['lastmodified'],
                      page['etag'],
                      page['redirect']))
        conn.commit()
        curs.close()

    def run(self):
        while True:
            try:
                # block for 1 second,
                # if page is None, then stop
                page = self.in_queue.get(True, 1)
                if not page:
                    self.logger.info('sql worker receive stop signal')
                    break
                self.dump_page(page)
            except Empty, e:
                continue
            except Exception, e:
                self.logger.error('%s %s url=%s' %
                                  (e.__class__.__name__, e,
                                   page['url']))
                break
        self.conn.close()


class Spider(object):
    """Simple spider grabs data from the url."""

    def __init__(self,
                 url,
                 depth=1,
                 logfile=None, loglevel=None,
                 threads=1,
                 dbfile=None,
                 key=''):
        """init Spider but will not start automatically.

        param: url If scheme is not specified, http will be used
        param: depth How deep the spider will dive into
        param: logfile Path to the log file
        param: loglevel Integer in [1,5]=>[critical,error,warning,
                        info,debug]
        param: threads Number of parallel thread to crawl page
        param: dbfile Path to sqlite3 database file
        param: key Regular expression to search page content
        """
        self.logger = self.get_logger(logfile, loglevel)
        self.url_pattern = self.compile_url_pattern()
        self.key = self.get_key_pattern(key)
        self.url = self.get_abs_url(None, url)
        self.depth = depth
        self.tasks_queue = Queue()
        self.output_queue = Queue()
        self.pool = TreadPool(threads)
        self.sql_worker = SQLWorker(dbfile, self.output_queue,
                                    self.logger)
        self.progress_urls = []
        self.status_timer = Timer(10.0, self.print_status)

    def compile_url_pattern(self, pattern=None, verbose=None):
        """Compile a new url pattern.

        By default, some valid url will be ignored because i don't
        want it. i.e. 'home' is valid but will be denied.
        """
        if not pattern:
            pattern = r"""
                 ^ # match from beginning
                 (http://|https://) # may starts with http or https
                 [^\b]+ # following some characters
                 $ # match the end
                 """
            verbose = re.VERBOSE
        if not verbose:
            self.url_pattern = re.compile(pattern)
        else:
            self.url_pattern = re.compile(pattern, verbose)
        return self.url_pattern

    def convert_to_unicode(self, source):
        """Return converted string from source to unicode string

        If source cannot be converted, then just return itself.
        Currently, only support utf-8, gb2312, gbk.
        """
        charsets = ('utf-8', 'gb2312', 'gbk')
        for charset in charsets:
            try:
                s = source.decode(charset)
            except Exception, e:
                self.logger.debug(
                    'try to convert from %s to unicode failed. %s' %
                    (charset, source[:500]))
                continue
            return s
        # this is an error because sqlite3 will stop working
        self.logger.error('try to convert to unicode failed. %s' %
                          source[:500])
        raise Exception('Cannot unicode the content')

    def crawl_page(self, url, depth):
        """Dump all data from url and also his child link.

        If you want to dump child link, depth should greater than 1.
        """
        result = self.get_page(url)
        if not result:
            return
        self.logger.info('get content from %s done' % url)
        # if depth is done then stop
        if depth <= 1:
            return
        links = self.get_all_links(result['content'])
        links = self.filter_links(links, url)
        # put links into queue
        for link in links:
            self.tasks_queue.put((link, depth - 1))

    def filter_links(self, links, parent=None):
        """Filter links

        1. remove fragment
        2. transform to absolute url
        3. remove duplicated urls
        4. remove invalid urls
        """
        # remove urls' fragment
        l = [urllib2.urlparse.urldefrag(link)[0]
             for link in links]
        # get urls' absolute url
        l = [self.get_abs_url(parent, link) for link in l]
        # remove the reduplicated links
        l = list(set(l))
        # remove invalid urls
        l = [link for link in l if self.is_valid_url(link)]
        return l

    def get_abs_url(self, base, url):
        """Get absolute address from url based on the base url.

        Using urllib2.urlparse.urljoin(). Note, the result can be
        unreachable!

        If base is not a valid url can be accessed via urllib2, then
        it will be tread as None.

        If finally abs url has no scheme, 'http' will be used.

        Example:
        return http://a/b if base = http://a/ and url = b
        return http://b if base = http://a and url = b
        return http://c if base = http://a/b and url = /c
        return http://d if base = http://a and url = http://d
        """
        # if base is not valid, then this must be called by user
        if base and not self.is_valid_url(base):
            self.logger.warn('invalid base url %s' % base)
            base = None
        abs_url = urllib2.urlparse.urljoin(base, url)
        if not self.is_valid_url(abs_url):
            # only try the http scheme, ignore the https case
            self.logger.warn('missing scheme for %s, set to http' %
                             abs_url)
            pr = urllib2.urlparse.urlparse(abs_url)
            if not pr.scheme:
                abs_url = ''.join(('http://', abs_url))
        return abs_url

    def get_all_links(self, content):
        """Get all links from content's 'a' tags."""
        soup = BeautifulSoup.BeautifulSoup(content)
        links = []
        for link in soup('a'):
            for attr in link.attrs:
                if attr[0] == 'href':
                    links.append(attr[1].strip())
        return links

    def get_key_pattern(self, key):
        """Get key pattern, convert to unicode, using re.compile."""
        if not key:
            return None
        try:
            unicode_key = self.convert_to_unicode(key)
        except Exception, e:
            msg = 'unrecognized searching key encoding, set to none.'
            msg = '%s %s' % (msg, e)
            self.logger.error(msg)
            return None
        else:
            return re.compile(unicode_key)

    def get_logger(self, logfile, loglevel):
        """Return a logger named class.name.

        if logfile is None, it will output to console.
        if loglevel is an integer of [1,5], more details if larger.
        """
        logger = logging.getLogger(__name__)
        if not logfile:
            log_handler = logging.StreamHandler()
        else:
            log_handler = logging.FileHandler(logfile)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)
        ll = (logging.CRITICAL,
              logging.ERROR,
              logging.WARNING,
              logging.INFO,
              logging.DEBUG)
        if not loglevel in range(1, 6):
            loglevel = 5
        logger.setLevel(ll[loglevel - 1])
        log_handler.setLevel(ll[loglevel - 1])
        logger.addHandler(log_handler)
        return logger

    def get_page(self, url):
        """Get content from page and safely close the connection."""
        try:
            req = urllib2.Request(url)
            # using gzip to accelerate
            req.add_header('Accept-encoding', 'gzip')
            opener = urllib2.build_opener()
            page = opener.open(req, None, timeout=30)
            try:
                result = self.verify_page_headers(page.headers)
                result['content'] = self.get_page_content(page)
            except Exception, e:
                page.close()
                raise e
            result['url'] = url
            # this url has been redirected, what's up?
            if hasattr(page, 'url'):
                result['redirect'] = page.url
            else:
                result['redirect'] = ''
            page.close()
            # if key is defined, then only dump page contains key
            if self.key:
                if self.key.search(result['content']):
                    self.output_queue.put(result)
            else:
                self.output_queue.put(result)
        # ignore any exception, just log it, and return None
        except Exception, e:
            self.logger.error(
                'url %s is unreachable. Exception %s %s' %
                (url, e.__class__.__name__, e))
            result = None
        return result

    def get_page_content(self, page):
        """Get content from page.

        raise Exception if can't convert content to unicode
        """
        # unpack gzip file
        if page.headers.get('content-encoding', '') == 'gzip':
            page_content = page.read()
            fileobj = StringIO.StringIO(page_content)
            zipfile = gzip.GzipFile(fileobj=fileobj)
            content = zipfile.read()
        else:
            content = page.read()
        # try to convert to unicode because i don't want to kill
        # sqlite3 and myself
        content = self.convert_to_unicode(content)
        return content

    def is_valid_url(self, url):
        """Verify if the url can be searched by url_pattern attr."""
        return True if self.url_pattern.search(url) else False

    def print_status(self):
        """Print status of time and undone tasks."""
        undone_urls = self.tasks_queue.qsize()
        undone_urls += self.pool.undone_tasks()
        print 'current time: %s' % time.strftime('%Y-%m-%d %H:%M:%S'),
        print 'unprocessed count of urls = %d' % undone_urls
        # yoho, another timer, i always like the new one
        self.status_timer = Timer(10.0, self.print_status)
        self.status_timer.start()

    def start(self):
        """Start to crawl page"""
        print 'start at %s' % time.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info('task start from %s with depth %s' %
                         (self.url, self.depth))
        self.sql_worker.start()
        self.status_timer.start()
        self.tasks_queue.put((self.url, self.depth))
        try:
            while True:
                try:
                    # block for 1 second
                    url, depth = self.tasks_queue.get(True, 1)
                except Empty, e:
                    # oops, some task is not done yet
                    if self.pool.undone_tasks():
                        # go back to work, fool!
                        continue
                    # oh, great, i can stop
                    else:
                        # tell the sql worker that he can go home
                        self.output_queue.put(None)
                        # break out to finally block
                        break
                # avoid reduplicated urls
                if not url in self.progress_urls:
                    self.pool.spawn(self.crawl_page, *(url, depth))
                    self.progress_urls.append(url)
        except Exception, e:
            self.logger.critical('%s %s' % (e.__class__.__name__, e))
        finally:
            # block for all pool slot done
            self.pool.joinall()
            # block for sql dump
            self.sql_worker.join()
            # stop timer
            self.status_timer.cancel()
            print 'stop at %s' % time.strftime('%Y-%m-%d %H:%M:%S')
            print 'process url count=%d' % len(self.progress_urls)
            self.logger.info('task done!')

    def verify_page_headers(self, headers):
        """Verify some information of a page's headers.

        Including page's etag, last-modified
        raise Exception if the page is not text file
        """
        result = {}
        # ignore non-text file
        content_type = headers.get('Content-Type', '')
        if not content_type.startswith('text'):
            raise Exception('invalid content-type')
        result['etag'] = headers.get('ETag', '')
        result['lastmodified'] = headers.get('Last-Modified', '')
        return result


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url',
                        help='valid url for spider')
    parser.add_argument('-d', '--depth',
                        type=int, default=1,
                        help='depth in [1, 3) for spider')
    parser.add_argument('-f', '--logfile', default='/tmp/spider.log',
                        help='log file path')
    h = 'log level in [1,5], more detail for larger number'
    parser.add_argument('-l', '--loglevel', help=h,
                        type=int, choices=range(1, 6), default=5)
    parser.add_argument('--testself', action='store_true',
                        help='program self test')
    parser.add_argument('--thread', type=int, default=10,
                        help='parallel thread to grab data')
    parser.add_argument('--dbfile', help='file path for sqlite')
    parser.add_argument('--key', help='filter key for page content')
    args = parser.parse_args(argv)
    if args.testself:
        # i don't like doctest because it is fool to add test in codes
        # i know the following code is fool too, but i think it's
        # better to use unittest than doctest
        source_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(source_dir)
        sys.path.append(root_dir)
        from tests import test_spider
        suite = unittest.TestLoader().loadTestsFromModule(
            test_spider)
        unittest.TextTestRunner(verbosity=2).run(suite)
    spider = Spider(url=args.url,
                    depth=args.depth,
                    logfile=args.logfile, loglevel=args.loglevel,
                    threads=args.thread,
                    dbfile=args.dbfile,
                    key=args.key)
    spider.start()


if __name__ == '__main__':
    main()
