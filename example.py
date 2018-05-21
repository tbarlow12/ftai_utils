from ftai_utils.image_repos import FTAI_Images, KaggleImages, ImageNetImages
import shutil

# TODO In order to use the library, you will need to move the file
# using it outside of the directory (one level up).

ftai_dir = 'ftai_images'
kaggle_dir = 'kaggle_images'
imagenet_dir = 'imagenet_images'

# Clear out local directories
shutil.rmtree(ftai_dir)
shutil.rmtree(kaggle_dir)
shutil.rmtree(imagenet_dir)

ftai = FTAI_Images()
kaggle = KaggleImages()
imagenet = ImageNetImages()

# Downloads images from a container to a local directory
# For ftai, it defaults to the 'approved' container. You shouldn't
# need to worry about any other containers. Both kaggle and imagenet
# are their own containers
ftai.download_images(ftai_dir)
kaggle.download_images(kaggle_dir)
imagenet.download_images(imagenet_dir)

# TODO Do whatever you need to do with the images here 
# (train tensorflow, evaluate models, etc.)

# This marks the images as processed in the DB as well as moves them
# to the 'processed' container in blob storage
ftai.dir_processed(ftai_dir)
