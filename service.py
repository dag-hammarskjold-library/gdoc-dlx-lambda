import requests
import boto3
import json
import datetime
import hashlib
import os
from io import BytesIO
from zipfile import ZipFile
from dlx import DB
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
from dlx.file.s3 import S3
from dlx.util import ISO6391
from gdoc import GDOC, GDOCEntry, encode_fn

def handler(event, context):
    # set dynamic params

    print(event)

    minutes_ago = datetime.timedelta(minutes=1440)
    if 'minutes-ago' in event:
        minutes_ago = datetime.timedelta(minutes=event['minutes-ago'])
    
    date_to = datetime.datetime.now()
    date_from = date_to - minutes_ago
    if 'date-from' in event:
        if 'date-to' in event:
            date_to = event['date-to']
            date_from = event['date-from']
        else:
            print("Date To is required if using Date From.")
            return {
                'status_code': 400,
                'message': 'Invalid datee selections. Date To is required if using Date From.'
            }

    ssm_client = boto3.client('ssm')
    parameter_name = 'connect-string'
    if 'connect-string-parameter-name' in event:
        parameter_name = event['connect-string-parameter-name']

    db_connect = ssm_client.get_parameter(Name=parameter_name)['Parameter']['Value']
    db_client = DB.connect(db_connect)
    creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])
    gdoc_api_secrets = json.loads(ssm_client.get_parameter(Name='gdoc-api-secrets')['Parameter']['Value'])
    gdoc_url = 'https://conferenceservices.un.org/ICTSAPI/ODS/GetODSDocumentsV2'

    S3.connect(
        creds['aws_access_key_id'], creds['aws_secret_access_key'], creds['bucket']
    )

    try:
        duty_station = event['duty-station']
    except KeyError:
        duty_station = 'NY'
    #for duty_station in ['NY', 'GE']:
    print("Getting metadata for files released in {} from {} to {}".format(duty_station, date_from, date_to))
    gdoc = GDOC(gdoc_url, gdoc_api_secrets, duty_station, date_from, date_to)
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
                        source='gDoc::{}'.format(duty_station)
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