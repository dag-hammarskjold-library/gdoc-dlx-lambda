import argparse
import boto3
import json
import sys
import mimetypes
from datetime import date
from langs import LANGS
from dlx import DB
from dlx.file.s3 import S3
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
from gdoc import encode_fn

'''
This script expects a filename, a symbol, and a language. Put quotes around anything that has spaces in it.
'''

parser = argparse.ArgumentParser(description='Run a dlx.files import process ad-hoc for one file, providing a symbol and language code (single letter).')
parser.add_argument('filename', metavar='filename', type=str)
parser.add_argument('symbol', metavar='symbol', type=str)
parser.add_argument('language', metavar='language', type=str)

args = parser.parse_args()

right_now = str(date.today())
sys.stdout = open('logs/event-{}.log'.format(right_now), 'a+', buffering=1)

ssm_client = boto3.client('ssm')
db_connect = ssm_client.get_parameter(Name='connect-string')['Parameter']['Value']
db_client = DB.connect(db_connect)
creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])

S3.connect(
    creds['aws_access_key_id'], creds['aws_secret_access_key'], creds['bucket']
)

print("Processing {}".format(args.filename))
file_handle = open(args.filename, 'rb')

extension = args.filename.split('.')[-1]


try:
    encoded_filename = encode_fn(args.symbol, LANGS[args.language], extension)
    print(encoded_filename)
    identifiers = []
    identifiers.append(Identifier('symbol',args.symbol))
    imported = File.import_from_handle(
            handle=file_handle,
            filename=args.filename,
            identifiers=identifiers,
            languages=[LANGS[args.language]], 
            mimetype=mimetypes.guess_type(args.filename)[0], 
            source='adhoc::digitization'
        )
    print("Imported {}".format(imported))
except FileExists:
    print("File already exists in the database. Continuing.")
    pass
except:
    raise