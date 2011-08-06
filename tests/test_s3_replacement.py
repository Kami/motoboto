# -*- coding: utf-8 -*-
"""
test_s3_replacement.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and $NAME
"""
import logging
import os
import sys
import unittest

if os.environ.get("USE_MOTOBOTO", "0") == "1":
    import motoboto as boto
else:
    import boto

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

class TestS3(unittest.TestCase):
    """test S3 functionality"""

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()        
        log.debug("opening s3 connection")
        self._s3_connection = boto.connect_s3()

    def tearDown(self):
        log = logging.getLogger("tearDown")
        if hasattr(self, "_s3_connection") \
        and self._s3_connection is not None:
            log.debug("closing s3 connection")
            try:
                self._s3_connection.close()
            except AttributeError:
                # 2011-08-04 dougfort -- boto 2.0 chokes if there are no
                # open http connections
                pass
            self._s3_connection = None

    def test_bucket(self):
        """
        test basic bucket handling
        """
        bucket_name = "com.dougfort.test_bucket"
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "buckets", bucket_list
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    _initialize_logging()
    unittest.main()

