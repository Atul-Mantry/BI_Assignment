import os
import logging

logging.basicConfig(level=logging.DEBUG)

os.chdir('/home/atulmantry1992/')
print(os.getcwd())
try:
        os.system('gcsfuse --implicit-dirs --debug_gcs --debug_fuse bux-bi-assignment-amantry scripts/gcs_bucket')
except Exception as e:
        pass