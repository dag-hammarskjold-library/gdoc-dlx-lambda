import argparse
from datetime import date, datetime, timedelta
import boto3
import json
import sys
import os
import re
from langs import LANGS
from dlx import DB
from dlx.file.s3 import S3
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
from gdoc import GDOCEntry, GDOC, encode_fn

'''
Note: This is expecting a list of filenames in the format {symbol}-{language}.{ext}
Where symbol is encoded simply to replace / with _ and language is in [ACEFRS]

Example: A_CONF.10_L.3-E.pdf
'''

parser = argparse.ArgumentParser(description='Run a gDoc process ad-hoc for one a folder of files, with modified symbols as filenames.')
parser.add_argument('folder', metavar='folder', type=str)

right_now = str(date.today())
sys.stdout = open('event-{}.log'.format(right_now), 'a+', buffering=1)

args = parser.parse_args()

minutes_ago = timedelta(minutes=1440)
date_to = datetime.now()
date_from = date_to - minutes_ago

ssm_client = boto3.client('ssm')
db_connect = ssm_client.get_parameter(Name='connect-string')['Parameter']['Value']
db_client = DB.connect(db_connect)
creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])


S3.connect(
    creds['aws_access_key_id'], creds['aws_secret_access_key'], creds['bucket']
)

for root, dirs, files in os.walk(args.folder):
    for filename in files:
        print("Processing {}".format(filename))
        file_handle = open(root + '/' + filename, 'rb')
        raw_sym_lang = ".".join(filename.split('.')[:-1])
        raw_sym, lang = raw_sym_lang.split('-')
        #print(raw_sym)
        re_sym = re.sub('_','/',raw_sym)
        print("Found {}".format(re_sym))
        try:
            encoded_filename = encode_fn(re_sym, LANGS[lang], 'pdf')
            print(encoded_filename)
            identifiers = []
            identifiers.append(Identifier('symbol',re_sym))
            imported = File.import_from_handle(
                    handle=file_handle,
                    filename=filename,
                    identifiers=identifiers,
                    languages=[LANGS[lang]], 
                    mimetype='application/pdf', 
                    source='adhoc::digitization'
                )
            print("Imported {}".format(imported))
        except FileExists:
            print("File already exists in the database. Continuing.")
            pass
        except:
            raise