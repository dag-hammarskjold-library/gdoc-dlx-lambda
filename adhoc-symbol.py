import argparse
from datetime import date, datetime, timedelta
import boto3
import json
from dlx import DB
from dlx.file.s3 import S3
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
from gdoc import GDOCEntry, GDOC, encode_fn

parser = argparse.ArgumentParser(description='Run a gDoc process ad-hoc for one symbol.')
parser.add_argument('symbol', metavar='symbol', type=str)

#right_now = str(date.today())
#sys.stdout = open('event-{}.log'.format(right_now), 'a+', buffering=1)

args = parser.parse_args()

minutes_ago = timedelta(minutes=1440)
date_to = datetime.now()
date_from = date_to - minutes_ago

ssm_client = boto3.client('ssm')
db_connect = ssm_client.get_parameter(Name='connect-string')['Parameter']['Value']
db_client = DB.connect(db_connect)
creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])
gdoc_api_secrets = json.loads(ssm_client.get_parameter(Name='gdoc-api-secrets')['Parameter']['Value'])
gdoc_url = 'https://conferenceservices.un.org/ICTSAPI/ODS/GetODSDocumentsV2'

S3.connect(
    creds['aws_access_key_id'], creds['aws_secret_access_key'], creds['bucket']
)

# First try to get the file from NY. 

print("Getting metadata for files released in {} for {}".format('NY', args.symbol))
gdoc = GDOC(gdoc_url, gdoc_api_secrets, 'NY', None, None, args.symbol)
metadata = gdoc.metadata_only()
for entry in metadata:
    print(entry.__str__())
    print("Fetching files for symbol {}".format(entry.id))
    zipfile = gdoc.get_files_by_symbol(entry.id)
    #for ext in ['pdf', 'docx']:
    for f in entry.files:
        try:
            got_file = zipfile.open(f['filename'], 'r')
            try:
                filename = encode_fn(entry.symbols, f['language'], 'pdf')
                print(filename)
                identifiers = []
                for s in entry.symbols:
                    identifiers.append(Identifier('symbol',s))
                imported = File.import_from_handle(
                    handle=got_file,
                    filename=filename,
                    identifiers=identifiers,
                    languages=[f['language']], 
                    mimetype='application/pdf', 
                    source='gDoc::{}'.format('NY')
                )
                print("Imported {}".format(imported))
            except FileExists:
                print("File already exists in the database. Continuing.")
                pass
            except:
                raise
        except KeyError:
            print("MissingFileException: File {} was not found in the archive for {}.".format(f['filename'], entry.symbols[0]))
            next