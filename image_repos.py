import os
from os import listdir
from os.path import isfile, join
import threading
from azure.storage.blob.blockblobservice import BlockBlobService
import pyodbc
import pdb

def files_in_dir(dir):
    return [f for f in listdir(dir) if isfile(join(dir, f))]

def blob_name_to_local(blob_name):
    return blob_name.replace('/', '-')

def local_to_blob_name(local):
    return local.replace('-', '/')

class ImageRepo(object):

    def download_image(self, blob_name, dest_path, container_name=None):
        if container_name is None:
            container_name = self.source_container
        dest_path = dest_path.strip()
        if len(dest_path) == 0:
            dest_path = '.'
        if dest_path[-1] != '/':
            dest_path += '/'
        local_path = dest_path + blob_name_to_local(blob_name)
        print('Saving blob to path: ' + local_path)
        self.blob_service.get_blob_to_path(
            container_name, 
            blob_name, 
            local_path)

    def download_images(self, dest_path, container_name=None, tag=None):
        if container_name is None:
            container_name = self.source_container
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)
        tag_prefix = tag
        if tag_prefix is not None and tag_prefix[-1] != '/':
            tag_prefix += '/'
        blobs_in_container = self.blob_service.list_blobs(container_name, prefix=tag_prefix)
        threads = []
        for blob in blobs_in_container:
            t = threading.Thread(target=self.download_image, args=(blob.name, dest_path, container_name,))
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

    def move_blob(self, blob_name, from_container, to_container):
        blob_url = self.blob_service.make_blob_url(from_container, blob_name)
        self.blob_service.copy_blob(to_container, blob_name, blob_url)
        self.blob_service.delete_blob(from_container, blob_name)

    def __init__(self):
        account = os.environ['BLOB_ACCOUNT_NAME']
        key = os.environ['BLOB_ACCOUNT_KEY']
        self.blob_service = BlockBlobService(account, key)    

class ImageNet(ImageRepo):

    source_container = 'image-net'

class UserImages(ImageRepo):

    source_container = 'approved-images-dev'
    dest_container = 'processed-images-dev'

    def image_processed(self, file_name):
        self.move_blob(
            local_to_blob_name(file_name), 
            self.source_container, 
            self.dest_container)

    def dir_processed(self, dir):
        files = files_in_dir(dir)
        threads = []
        for f in files:
            blob_name = local_to_blob_name(f)
            t = threading.Thread(target=self.image_processed, args=blob_name,)
            threads.append(t)
            t.start()
        for thread in threads:
            thread.join()

class KaggleImages(ImageRepo):

    source_container = 'test'

class SQLWrapper(object):

    def __init__(self):
        self.server = os.environ['SQL_SERVER']
        self.database = os.environ['SQL_DB']
        self.username = os.environ['SQL_USERNAME']
        self.password = os.environ['SQL_PASSWORD']
        self.driver = '{ODBC Driver 13 for SQL Server}'

    def execute_read(self, command):

        cnxn = pyodbc.connect('DRIVER='+self.driver+';SERVER='+self.server+';PORT=1443;DATABASE='+self.database+';UID='+self.username+';PWD='+ self.password)
        cursor = cnxn.cursor()
        cursor.execute(command)
        row = cursor.fetchone()
        while row:
            print(str(row))
            row = cursor.fetchone()

    