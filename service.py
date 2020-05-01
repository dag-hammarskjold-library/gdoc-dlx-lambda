import requests
import boto3
import json
import datetime
import hashlib
from io import BytesIO
from zipfile import ZipFile

def get_gdoc(additional_params):
    # Set variables
    ssm_client = boto3.client('ssm')
    gdoc_api_secrets = json.loads(ssm_client.get_parameter(Name='gdoc-api-secrets')['Parameter']['Value'])

    gdoc_url = 'https://conferenceservices.un.org/ICTSAPI/ODS/GetODSDocumentsV2'

    params = {}
    params.update(gdoc_api_secrets)
    params.update(additional_params)

    try:
        res = requests.get(
            url = gdoc_url,
            params = params
        )
        print(res.request.url)
        return res
    except:
        raise

def get_metadata(content, prefix):
    try:
        zipfile = ZipFile(BytesIO(content))
        if 'export.txt' in zipfile.namelist():
            metadata = json.load(zipfile.open('export.txt'))
            return_files = {
                'docx': '',
                'pdf': ''
            }
            for r in metadata:
                #print(r)
                jobnumber = r['odsNo']
                docx = "{}{}.docx".format(prefix,jobnumber)
                pdf = "{}{}.pdf".format(prefix,jobnumber)
                if docx in zipfile.namelist():
                    return_files['docx'] = zipfile.open(docx)
                if pdf in zipfile.namelist():
                    return_files['pdf'] = zipfile.open(pdf)
            return metadata, return_files
        else:
            return None
    except:
        raise

def handler(event, context):
    # set dynamic params

    five_minutes = datetime.timedelta(minutes=60*24)
    date_to = datetime.datetime.now()
    date_from = date_to - five_minutes
    #print(date_from)
    #print(date_to)

    additional_params = {
        'AppName': 'gDoc',
        'DstOff': 'Y',
        'Odsstatus': 'Y',
        'DownloadFiles': 'Y',
        'LocalDate': datetime.datetime.now().__str__(),
        'ResultType': 'Released',
        'DateFrom': date_from,
        'DateTo': date_to,
        'DutyStation': '',
    }

    # Get gdoc files and metadata
    additional_params['DutyStation'] = 'NY'
    res = get_gdoc(additional_params)
    if res:
        # parse gdoc export.txt
        metadata, files = get_metadata(res.content, 'N')
        print("Got {} entries.".format(len(metadata)))
        docx, pdf = files
        if docx != '':
            md5sum = hashlib.md5(docx).hexdigest
            print(md5sum)
        if pdf != '':
            md5sum = hashlib.md5(pdf).hexdigest
            print(md5sum)