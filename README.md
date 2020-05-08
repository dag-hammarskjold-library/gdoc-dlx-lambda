### Getting Started
* Clone repository and setup your AWS configuration if you haven't already.
* Create your virtual environment and activate it.
* In your terminal, navigate to the gdoc-dlx-lambda directory.
* `pip install -r requirements.txt`

You have options on how to invoke this in a non-Lambda environment for validation.

`lambda invoke` will use the contents of event.json to run a test event. This file contains some directives to instruct the function to poll gDoc for the last 24 hours and write the results to the development database.

`lambda invoke --event-file prod-event.json` will do the same as the above, but against the production database. 

You can use these invocation events to control the timeframe for one-off script runs as well, but keep in mind that larger time differences will result in longer script runs, memory usage, and data transfer. If you're going to do big batches, probably it's better to write a special script to handle that. This should get you started, though.
