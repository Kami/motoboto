# -*- coding: utf-8 -*-
"""
test_s3_replacement.py

test that motoboto can replace boto for s3 functions

note that you need credentials for both AWS and $NAME
"""
import filecmp
import logging
import os
import os.path
import random
import shutil
import sys
import unittest

if os.environ.get("USE_MOTOBOTO", "0") == "1":
    import motoboto as boto
    from motoboto.s3.key import Key
else:
    import boto
    from boto.s3.key import Key

_tmp_path = os.environ.get("TEMP", "/tmp")
_test_dir_path = os.path.join(_tmp_path, "test_s3_replacement")

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

def _random_string(size):
    return "".join([chr(random.randint(0, 255)) for _ in xrange(size)])

class TestS3(unittest.TestCase):
    """test S3 functionality"""

    def setUp(self):
        log = logging.getLogger("setUp")
        self.tearDown()  
        log.debug("creating %s" % (_test_dir_path))
        os.makedirs(_test_dir_path)
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

        if os.path.exists(_test_dir_path):
            shutil.rmtree(_test_dir_path)

    def xxxtest_bucket(self):
        """
        test basic bucket handling
        """
        bucket_name = "com.dougfort.test_bucket"

        # list all buckets, ours shouldn't be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertFalse(bucket_in_list)

        # create the bucket
        new_bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(new_bucket is not None)
        self.assertEqual(new_bucket.name, bucket_name)
        
        # list all buckets, ours should be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertTrue(bucket_in_list)

        # create a duplicate bucket
        # s3 accepts this
        x = self._s3_connection.create_bucket(bucket_name)
        self.assertEqual(x.name, new_bucket.name)

        # list all buckets, ours should be there
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertTrue(bucket_in_list)

        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
        # list all buckets, ours should be gone
        bucket_in_list = False
        bucket_list = self._s3_connection.get_all_buckets()
        print >> sys.stderr, "bucket list"
        for bucket in bucket_list:
            print >> sys.stderr, "    ", bucket.name
            if bucket.name == bucket_name:
                bucket_in_list = True
        self.assertFalse(bucket_in_list)

    def xxxtest_key_with_strings(self):
        """
        test simple key 'from_string' and 'as_string' functions
        """
        bucket_name = "com.dougfort.test_key_with_strings"
        key_name = u"test key"
        test_string = _random_string(1024)

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        self.assertFalse(write_key.exists())

        # upload some data
        write_key.set_contents_from_string(test_string)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        returned_string = read_key.get_contents_as_string()      
        self.assertEqual(returned_string, test_string)

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def xxxtest_key_with_files(self):
        """
        test simple key 'from_file' and 'to_file' functions
        """
        log = logging.getLogger("test_key_with_files")
        bucket_name = "com.dougfort.test_key_with_files"
        key_name = "A" * 1024
        test_file_path = os.path.join(
            _test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(_random_string(buffer_size))
                bytes_written += buffer_size

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(archive_file)        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        retrieve_file_path = os.path.join(
            _test_dir_path, "test_key_with_files-orignal"
        )
        with open(retrieve_file_path, "w") as retrieve_file:
            read_key.get_contents_to_file(retrieve_file)      
        self.assertTrue(
            filecmp.cmp(test_file_path, retrieve_file_path, shallow=False)
        )

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
    def test_key_with_files_and_callback(self):
        """
        test simple key 'from_file' and 'to_file' functions
        """
        def _archive_callback(bytes_sent, total_bytes):
            print >> sys.stderr, "archived", str(bytes_sent), "out of", \
                    str(total_bytes)

        def _retrieve_callback(bytes_sent, total_bytes):
            print >> sys.stderr, "retrieved", str(bytes_sent), "out of", \
                    str(total_bytes)

        log = logging.getLogger("test_key_with_files")
        bucket_name = "com.dougfort.test_key_with_files"
        key_name = "A" * 1024
        test_file_path = os.path.join(
            _test_dir_path, "test_key_with_files-orignal"
        )
        test_file_size = 1024 ** 2
        buffer_size = 1024

        log.debug("writing %s bytes to %s" % (
            test_file_size, test_file_path, 
        ))
        bytes_written = 0
        with open(test_file_path, "w") as output_file:
            while bytes_written < test_file_size:
                output_file.write(_random_string(buffer_size))
                bytes_written += buffer_size

        # create the bucket
        bucket = self._s3_connection.create_bucket(bucket_name)
        self.assertTrue(bucket is not None)
        self.assertEqual(bucket.name, bucket_name)

        # create an empty key
        write_key = Key(bucket)

        # set the name
        write_key.name = key_name
        self.assertFalse(write_key.exists())

        # upload some data
        with open(test_file_path, "r") as archive_file:
            write_key.set_contents_from_file(
                archive_file, cb=_archive_callback
            )        
        self.assertTrue(write_key.exists())

        # create another key with the same name 
        read_key = Key(bucket, key_name)

        # read back the data
        retrieve_file_path = os.path.join(
            _test_dir_path, "test_key_with_files-orignal"
        )
        # 2011-08-08 dougfort boto aborts if you don't tell it the size
        read_key.size = test_file_size
        with open(retrieve_file_path, "w") as retrieve_file:
            read_key.get_contents_to_file(
                retrieve_file, cb=_retrieve_callback
            )      
        self.assertTrue(
            filecmp.cmp(test_file_path, retrieve_file_path, shallow=False)
        )

        # delete the key
        read_key.delete()
        self.assertFalse(write_key.exists())
        
        # delete the bucket
        self._s3_connection.delete_bucket(bucket_name)
        
if __name__ == "__main__":
    _initialize_logging()
    unittest.main()

