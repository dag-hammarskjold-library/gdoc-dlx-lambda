from service import handler
from datetime import timedelta, datetime, date
import sys

event = {
    "connect-string-parameter-name": "connect-string",
}

#today_date = date.today()
today_date = date(2020, 5, 1)
wayback_start_date = date(2018, 11, 1)
a_week_ago = timedelta(weeks=1)
this_start = today_date - a_week_ago
this_end = today_date

sys.stdout = open('event.log', 'w', buffering=1)

while this_start >= wayback_start_date:
    event['date-from'] = "{} 00:00:00".format(str(this_start))
    event['date-to'] = "{} 00:00:00".format(str(this_end))

    try:
        response = handler(event, context=None)
    except:
        raise

    # Decrement
    this_end = this_start
    this_start = this_end - a_week_ago