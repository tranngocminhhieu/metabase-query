# Metabase query
[![Downloads](https://img.shields.io/pypi/dm/metabase-query)](https://pypi.org/project/metabase-query)
[![Pypi](https://img.shields.io/pypi/v/metabase-query)](https://pypi.org/project/metabase-query)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](https://github.com/tranngocminhhieu/metabase-query/issues)
[![MIT](https://img.shields.io/github/license/tranngocminhhieu/metabase-query)](https://github.com/tranngocminhhieu/metabase-query/blob/main/LICENSE)

![example-table.png](https://raw.githubusercontent.com/tranngocminhhieu/metabase-query/main/example-table.png)

This package will help Data workers get data from [Metabase](https://www.metabase.com/) questions more easily and effectively. It only focuses on the [Card Query API](https://www.metabase.com/docs/latest/api/card#post-apicardcard-idqueryexport-format), [Dataset API](https://www.metabase.com/docs/latest/api/dataset#post-apidatasetexport-format) and does not include other Metabase APIs.

It is allowing you to execute queries using URLs or SQL directly within your Python code. Whether you are working with saved questions, SQL queries, or need to filter large datasets across multiple queries, this package offers a streamlined and flexible approach to retrieving data from your Metabase. With options for handling retries, connection limits, and custom filters, it’s built to handle complex querying needs with ease.


## Features
1. Get question data in any data format provided by Metabase (JSON, CSV, XLSX).
2. Input question URL and Metabase Session. No need to provide parameters payload.
3. JSON results have the same column sort order as the browser.
4. Automatically check if Metabase session is available.
5. Easy to filter data with a simple dict.
6. Allow retry if an error occurs due to server slowdown.
7. Allow entering multiple filter values in bulk.
8. Support both saved questions and unsaved questions.
9. Support SQL query.

## Installation
```shell
pip install --upgrade metabase-query
```

## Quick start example

### Create Metabase object

Simple syntax:

```python
from metabase_query import Metabase

mb = Metabase(metabase_session='YourMetabaseSession')
```
Full options syntax:
```python
mb = Metabase(metabase_session='YourMetabaseSession',  retry_errors=None, retry_attempts=3, limit_per_host=5, timeout=600, verbose=True, domain=None)
```
- `metabase_session`: Your Metabase Session.
- `retry_errors`: `None` to retry with any error, a list of errors to retry with these errors only, contain matching. Default is `None`.
- `retry_attempts`: 0 will not retry. Default is `3`.
- `limit_per_host`: The limit of connections per host. Default is `5`.
- `timeout`: Timeout in seconds for each connection. Default is `600`.
- `verbose`: Print log or not. Default is `True`.
- `domain`: Not required for queries with URL, SQL queries is required. Default is `None`.

### Query with any URL
Example URLs:
```python
url = 'https://your-domain.com/question/123456-example?created_at=past3months' # Saved question
url = 'https://your-domain.com/question#eW91cl9xdWVyeQ==...' # Table or unsaved SQL query
```

Don't need to find parameters from payload, just paste the URL from the browser.
```python
data = mb.query(url=url)
```
- `format`: Support JSON, CSV, XLSX formats. Default is `'json'`.

Add filters easily with a dict.
```python
filter = {
    'order_id': [123456, 456789, 789012], # Unlimited values in list, WOW!
    'status': 'Completed'
}
data = mb.query(url=url, filter=filter, filter_chunk_size=5000)
```
- `filter`: One dict for a list of dicts.
- `filter_chunk_size`: If you have a bulk value filter, the package will splits your values into chunks to send the requests, and then concat the results into a single data.

One URL with multiple filters.
```python
filters = [
    {'created_at': '2024-08-01~2024-08-05'},
    {'created_at': '2024-08-06~2024-08-10'},
    {'created_at': '2024-08-11~2024-08-15'}
]

results = mb.query(url=url, filter=filters)

# We support a function to combine JSON and CSV data. Use it if you see data sets are the same columns.
from metabase_query.utils import combine_results
data = combine_results(results=[r['data'] for r in results], format='json')
```
Many URLs.
```python
urls = [
    'https://your-domain.com/question/123456-example?created_at=2024-08-01~2024-08-05',
    'https://your-domain.com/question/123456-example?created_at=2024-08-06~2024-08-10',
    'https://your-domain.com/question/123456-example?created_at=2024-08-11~2024-08-15'
]
results = mb.query(url=urls)
```
A URL list with a filter list.
```python
urls = [
    'https://your-domain.com/question/123456-example',
    'https://your-domain.com/question/123456-example',
    'https://your-domain.com/question/123456-example'
]

filters = [
    {'created_at': '2024-08-01~2024-08-05'},
    {'created_at': '2024-08-06~2024-08-10'},
    {'created_at': '2024-08-11~2024-08-15'}
]

results = mb.query(url=urls, filter=filters)
```

### SQL query
```python
sql = '''
SELECT * FROM your_table LIMIT 1000
'''

database = '1-presto'

data = mb.sql_query(sql=sql, database=database, format='json')
```
- `sql`: One SQL query or a list of SQL queries.
- `database`: One database ID or a list or database IDs follow SQL list. Look at the database slug on the browser.

Many SQL queries.
```python
sql_1 = '''
SELECT * FROM your_table WHERE created_at BETWEEN DATE '2024-08-01' AND '2024-08-05'
'''

sql_2 = '''
SELECT * FROM your_table WHERE created_at BETWEEN DATE '2024-08-06' AND '2024-08-10'
'''

sqls = [sql_1, sql_2]

results = mb.sql_query(sql=sqls, database=database)
```
---
Goood luck!