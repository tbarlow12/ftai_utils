import os
from azure.storage.blob.blockblobservice import BlockBlobService

class ImageRepo(object):

    def download_blob(self):
        pass

    def download_source_images(self):
        pass

    def upload_blob(self):
        pass

    def move_blob(self):
        pass

    def move_local(self):
        pass

    def delete_blob(self):
        pass

    def delete_local(self):
        pass

    def __init__(self):
        account = os.environ['BLOB_ACCOUNT_NAME']
        key = os.environ['BLOB_ACCOUNT_KEY']
        self.blob_service = BlockBlobService(account, key)
    

class ImageNet(ImageRepo):

    source_container = 'image-net'

class UserImages(ImageRepo):

    source_container = 'approved-images'
    dest_container = 'processed-images'

class KaggleImages(ImageRepo):

    source_container = 'test'


    