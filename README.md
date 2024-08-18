# Metabase query
[![Downloads](https://img.shields.io/pypi/dm/metabase-query)](https://pypi.org/project/metabase-query)
[![Pypi](https://img.shields.io/pypi/v/metabase-query)](https://pypi.org/project/metabase-query)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](https://github.com/tranngocminhhieu/metabase-query/issues)
[![MIT](https://img.shields.io/github/license/tranngocminhhieu/metabase-query)](https://github.com/tranngocminhhieu/metabase-query/blob/main/LICENSE)

![example-table.png](https://raw.githubusercontent.com/tranngocminhhieu/metabase-query/main/example-table.png)

metabase-query is a Python package designed to simplify data retrieval from Metabase, specifically focusing on the [Card Query API](https://www.metabase.com/docs/latest/api/card#post-apicardcard-idqueryexport-format) and [Dataset API](https://www.metabase.com/docs/latest/api/dataset#post-apidatasetexport-format). This package allows data professionals to execute queries using URLs or SQL directly within their Python code, facilitating streamlined access to Metabase data.

## Key features
- **Flexible Data Retrieval**: Retrieve data in any format supported by Metabase, including JSON, CSV, and XLSX.
- **Simple Integration**: Execute queries by simply inputting the question URL and Metabase session—no need to manually provide parameters.
- **Consistent Results**: JSON results maintain the same column order as displayed in the Metabase UI.
- **Session Management**: Automatically checks the availability of the Metabase session.
- **Effortless Filtering**: Easily apply filters to your queries using simple dictionaries.
- **Error Handling**: Supports automatic retries in case of server errors or slowdowns.
- **Bulk Filter Support**: Allows entering multiple filter values in a single request.
- **Query Versatility**: Supports both saved and unsaved questions, as well as SQL queries.

## Installation
To install the package, use the following pip command:
```shell
pip install --upgrade metabase-query
```

## Usage

### Basic Example
```python
from metabase_query import Metabase

# Initialize the MetabaseQuery object
mb = Metabase(metabase_session='YourMetabaseSession')

# Query data using a Metabase question URL
url = 'https://your-domain.com/question/123456-example?created_at=past3months'
data = mb.query(url=url, format='json')
```

#### Table URL and Unsaved question URL

```python
url = 'https://your-domain.com/question#eyJkYXRhc2V0X3F1ZXJ5Ijp7ImRhdGFiYXNlIjo2LCJxdWVyeSI6eyJzb3VyY2UtdGFibGUiOjQ4MzV9LCJ0eXBlIjoicXVlcnkifSwiZGlzcGxheSI6InRhYmxlIiwidmlzdWFsaXphdGlvbl9zZXR0aW5ncyI6e319'
data = mb.query(url=url, format='csv')

# Example saving data to a CSV file.
with open('data.csv', 'rb') as f:
    f.write(data)
```

### Advanced Settings
```python
mb = Metabase(metabase_session='YourMetabaseSession',  retry_errors=None, retry_attempts=3, limit_per_host=5, timeout=600, verbose=True, domain=None)
```
- `metabase_session`: Your Metabase Session.
- `retry_errors`: Set to `None` to retry on any error, or provide a list of specific errors to retry only for those. Default is `None`.
- `retry_attempts`: The number of retry attempts in case of an error. Default is `3`; set to `0` to disable retries.
- `limit_per_host`: The maximum number of connections allowed per host. Default is `5`.
- `timeout`: The timeout duration in seconds for each connection. Default is `600`.
- `verbose`: Whether to print logs. Default is `True`.
- `domain`: Not required for URL-based queries, but mandatory for SQL queries. Default is `None`.


### Working with Filters
#### Simple Filter
It will combine both filter in URL and filter dictionary. Priority filter dictionary if it exists on URL.
```python
filter = {
    'order_id': [123456, 456789, 789012], # UNLIMITED values!!!
    'status': 'Completed'
}
data = mb.query(url=url, filter=filter, filter_chunk_size=5000)
```
- `filter`: A single dictionary or a list of dictionaries representing the filters.
- `filter_chunk_size`: For bulk filter values, the package will divide the values into manageable chunks for processing, then combine the results into a single dataset.

#### Single URL with Multiple Filters
```python
filters = [
    {'created_at': '2024-08-01~2024-08-05'},
    {'created_at': '2024-08-06~2024-08-10'},
    {'created_at': '2024-08-11~2024-08-15'}
]

results = mb.query(url=url, filter=filters)

# Combine results if the datasets have the same columns
from metabase_query.utils import combine_results
data = combine_results(results=[r['data'] for r in results], format='json')
```

#### Multiple URLs
```python
urls = [
    'https://your-domain.com/question/123456-example?created_at=2024-08-01~2024-08-05',
    'https://your-domain.com/question/123456-example?created_at=2024-08-06~2024-08-10',
    'https://your-domain.com/question/123456-example?created_at=2024-08-11~2024-08-15'
]
results = mb.query(url=urls)
```

#### URL List with Filter List
```python
urls = [
    'https://your-domain.com/question/123456-example', # 1
    'https://your-domain.com/question/123456-example', # 2
    'https://your-domain.com/question/123456-example' # 3
]

filters = [
    {'created_at': '2024-08-01~2024-08-05'}, # For URL 1
    {'created_at': '2024-08-06~2024-08-10'}, # For URL 2
    {'created_at': '2024-08-11~2024-08-15'} # For URL 3
]

results = mb.query(url=urls, filter=filters)
```



### Executing SQL Queries
```python
sql = '''
SELECT * FROM your_table LIMIT 1000
'''

database = '1-presto'

data = mb.sql(sql=sql, database=database, format='json')
```
- `sql`: A single SQL query or a list of SQL queries.
- `database`: A single database ID or a list of database IDs corresponding to the SQL queries. Refer to the database slug in the browser for details.

#### Multiple SQL Queries
```python
sql_1 = '''
SELECT * FROM your_table WHERE created_at BETWEEN DATE '2024-08-01' AND '2024-08-05'
'''

sql_2 = '''
SELECT * FROM your_table WHERE created_at BETWEEN DATE '2024-08-06' AND '2024-08-10'
'''

sqls = [sql_1, sql_2]

results = mb.sql(sql=sqls, database=database)
```

## Contributing
Contributions are welcome! Please refer to the [issues page](https://github.com/tranngocminhhieu/metabase-query/issues) for ways you can help.

Good luck with your data queries!