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

LANGS = {
    'A': 'AR',
    'C': 'ZH',
    'E': 'EN',
    'F': 'FR',
    'G': 'DE',
    'R': 'RU',
    'S': 'ES'
}

class GDOCEntry(object):
    def __init__(self, symbols):
        self.id = symbols[0]
        self.symbols = symbols
        self.languages = []
        self.files = []

    def __str__(self):
        return_data = {
            'id': self.id,
            'symbols': self.symbols,
            'languages': self.languages,
            'files': self.files
        }
        return str(return_data)

class GDOC(object):
    def __init__(self, url, secrets, duty_station, date_from, date_to):
        self.url = url
        self.secrets = secrets
        self.basic_params = {
            'AppName': 'gDoc',
            'DstOff': 'Y',
            'Odsstatus': 'Y',
            'DutyStation': duty_station,
            'LocalDate': datetime.datetime.now().__str__(),
            'ResultType': 'Released',
            'DateFrom': date_from,
            'DateTo': date_to
        }
    
    def metadata_only(self):
        parameters = {
            'DownloadFiles': 'N',
        }
        parameters.update(self.secrets)
        parameters.update(self.basic_params)

        try:
            result = requests.get(
                url = self.url,
                params = parameters
            )
            zipfile = ZipFile(BytesIO(result.content))
            metadata = json.load(zipfile.open('export.txt'))
            symbol_list = []
            return_meta = []

            for entry in metadata:
                symbol1 = entry['symbol1']
                symbol2 = entry['symbol2']
                symbols = [symbol1, symbol2]
                if symbol1 not in symbol_list:
                    # We haven't seen this symbol yet
                    this_meta = GDOCEntry(symbols)
                    this_file = self.basic_params['DutyStation'][0] + str(entry['odsNo']) + '.pdf'
                    this_lang = LANGS[entry['languageId']]
                    this_meta.files.append({'filename': this_file, 'language': this_lang})
                    this_meta.languages.append(this_lang)
                    return_meta.append(this_meta)
                    symbol_list.append(symbol1)
                else:
                    # Symbol exists, so we can just update it
                    this_meta = list(filter(lambda x: x.id == symbol1, return_meta))[0]
                    this_file = self.basic_params['DutyStation'][0] + str(entry['odsNo']) + '.pdf'
                    this_lang = LANGS[entry['languageId']]
                    this_meta.files.append({'filename': this_file, 'language': this_lang})
                    this_meta.languages.append(this_lang)
                     
            return return_meta                
        except:
            raise

    def get_files_by_symbol(self, symbol):
        '''
        Returns a zipfile containing the files associated with a particular document symbol
        '''
        parameters = {
            'DownloadFiles': 'Y',
            'Symbol': symbol
        }
        parameters.update(self.secrets)
        parameters.update(self.basic_params)

        try:
            result = requests.get(
                url = self.url,
                params = parameters
            )
            zipfile = ZipFile(BytesIO(result.content))
            return zipfile
        except:
            raise

def handler(event, context):
    # set dynamic params

    print(event)

    minutes_ago = datetime.timedelta(minutes=5)
    if 'minutes-ago' in event:
        minutes_ago = datetime.timedelta(minutes=event['minutes-ago'])
    
    date_to = datetime.datetime.now()
    date_from = date_to - minutes_ago
    if 'dater-from' in event:
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

    for duty_station in ['NY', 'GE']:
        print("Getting metadata for files released in {} from {} to {}".format(duty_station, date_from, date_to))
        gdoc = GDOC(gdoc_url, gdoc_api_secrets, duty_station, date_from, date_to)
        metadata = gdoc.metadata_only()
        for entry in metadata:
            print(entry.__str__())
            print("Fetching files for symbol {}".format(entry.id))
            zipfile = gdoc.get_files_by_symbol(entry.id)
            #for ext in ['pdf', 'docx']:
            for f in entry.files:
                got_file = zipfile.open(f['filename'])
                try:
                    imported = File.import_from_handle(
                        handle=got_file,
                        filename=f['filename'],
                        identifiers=[Identifier('symbol', entry.symbols[0]), Identifier('symbol', entry.symbols[1])],
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
    return {
        'status_code': 200,
        'message': 'The operation completed successfully.'
    }