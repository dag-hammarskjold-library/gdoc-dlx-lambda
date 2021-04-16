from gdoc_api.scripts import gdoc_dlx
import sys

def handler(event, context):
    # This is necessary because the python-lambda commands get passed to the gdoc_dlx.run function
    sys.argv = [sys.argv[0]]
    
    try:
        duty_station = event["duty_station"]
    except:
        duty_station = "NY"
    try:
        date = event["date"]
    except:
        date = "2021-03-01"

    try:
        gdoc_dlx.run(station="NY", date="2021-03-01")
    except:
        raise

    return {
        'status_code': 200
    }