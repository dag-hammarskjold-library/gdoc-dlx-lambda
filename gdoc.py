

from dlx.util import ISO6391
from zipfile import ZipFile
from io import BytesIO
import requests
import datetime
import json
from langs import LANGS


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
    def __init__(self, url, secrets, duty_station='NY', date_from=None, date_to=None, symbol=None):
        
        self.url = url
        self.secrets = secrets
        self.basic_params = {
            'AppName': 'gDoc',
            'DstOff': 'Y',
            'Odsstatus': 'Y',
            #'DutyStation': duty_station,
            'LocalDate': datetime.datetime.now().__str__(),
            'ResultType': 'Released',
            #'DateFrom': date_from,
            #'DateTo': date_to
        }
        if duty_station is not None:
            self.basic_params.update({'DutyStation': duty_station})

        if date_from is not None:
            self.basic_params.update({'DateFrom': date_from})

        if date_to is not None:
            self.basic_params.update({'DateTo': date_to})

        if symbol is not None:
            self.basic_params.update({'Symbol': symbol})
    
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
                symbols = [symbol1]
                symbol2 = entry['symbol2']
                if len(symbol2) > 0:
                    symbols.append(symbol2)
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

def encode_fn(symbols, language, extension):
    
    ISO6391.codes[language.lower()]
    symbols = [symbols] if isinstance(symbols, str) else symbols
    xsymbols = [sym.translate(str.maketrans(' /[]*:;', '__^^!#%')) for sym in symbols]
    return '{}-{}.{}'.format('&'.join(xsymbols), language.upper(), extension)