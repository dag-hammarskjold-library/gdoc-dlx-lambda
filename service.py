from gdoc_api.scripts import gdoc_dlx
import sys, datetime

def handler(event, context):
    # This is necessary because the python-lambda commands get passed to the gdoc_dlx.run function
    sys.argv = [sys.argv[0]]

    print(f"Processing {event}")
    
    try:
        duty_station = event["duty_station"]
    except:
        duty_station = "NY"
    try:
        date = event["date"]
    except:
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        date = yesterday

    try:
        gdoc_dlx.run(station=duty_station, date=date, recursive=True)
    except:
        raise

    return {
        'status_code': 200
    }
