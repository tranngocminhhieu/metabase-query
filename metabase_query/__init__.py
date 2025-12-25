import asyncio
import aiohttp
from .card import Card
from .dataset import Dataset
from .sql import SQL
import sys
from tenacity import *
from .utils import raise_retry_errors, combine_results, define_url

if 'ipykernel' in sys.modules:
    import nest_asyncio
    nest_asyncio.apply()

class Metabase(object):
    def __init__(self, metabase_session=None, cookies=None, retry_errors=None, retry_attempts=3, limit_per_host=5, timeout=600, verbose=True, domain=None):
        '''
        Setting Metabase object.

        :param metabase_session: Your Metabase Session.
        :param retry_errors: None to retry with any error, a list of errors to retry with these errors only, contain matching. Default is None.
        :param retry_attempts: 0 will not retry. Default is 3.
        :param limit_per_host: The limit of connections per host. Default is 5.
        :param timeout: Timeout in seconds for each connection. Default is 600.
        :param verbose: Print log or not. Default is True.
        :param domain: Not required for queries with URL, SQL queries is required. Default is None.
        '''
        # Settings
        self.metabase_session = metabase_session
        self.retry_errors = retry_errors
        self.retry_attempts = retry_attempts
        self.limit_per_host = limit_per_host
        self.timeout = timeout
        self.verbose = verbose
        self.domain = domain
        self.cookies = cookies

        # Child classes
        self.Card = Card(metabase=self)
        self.Dataset = Dataset(metabase=self)
        self.SQL = SQL(metabase=self)

        # For printing log
        self.query_count = 0
        self.parse_count = 0

        if not metabase_session and not cookies:
            raise AttributeError('You must specify a metabase_session or cookies.')

    def print_if_verbose(self, *args):
        if self.verbose:
            print(*args)

    # Main 1
    def query(self, url, format='json', filter=None, filter_chunk_size=5000):
        '''
        Get data from any question URL, you can use a list of URLs or a list of filters to get data in bulk.

        :param url: One URL as string for a list of URLs.
        :param format: json, csv, xlsx.
        :param filter: One dict for a list of dicts.
        :param filter_chunk_size: If you have a bulk value filter, the package will splits your values into chunks to send the requests, and then concat the results into a single data.
        :return: One data or a list of data.
        '''
        self.query_count = 0
        self.parse_count = 0

        if filter_chunk_size < 1:
            raise ValueError('filter_chunk_size must be positive.')

        if format.lower() not in ['json', 'csv', 'xlsx']:
            raise ValueError('Metabase only supports JSON, CSV and XLSX formats.')

        result = asyncio.run(self.handle_urls(urls=url, format=format.lower(), filters=filter, filter_chunk_size=filter_chunk_size))

        return result

    # Main 2
    def sql(self, sql, database, format='json'):
        '''
        Get data from SQL queries, you can use a list of SQL queries to get data in bulk.

        :param sql: One SQL query or a list of SQL queries.
        :param database: One database ID or a list or database IDs follow SQL list. Look at the database slug on the browser.
        :param format: json, csv, xlsx.
        :return: One data or a list of data.
        '''
        self.query_count = 0
        self.parse_count = 0
        result = asyncio.run(self.SQL.query_sql(sqls=sql, databases=database, format=format.lower()))
        return result


    # Async for URL query
    async def handle_urls(self, urls, format='json', filters=None, filter_chunk_size=5000):
        '''
        Async allocation function for handling urls.

        :param urls: One URL or a list of URLs.
        :param format: json, csv, xlsx.
        :param filters: One dict for a list of dicts.
        :param filter_chunk_size: If you have a bulk value filter, the package will splits your values into chunks to send the requests, and then concat the results into a single data.
        :return:
        '''

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.limit_per_host), timeout=aiohttp.ClientTimeout(total=self.timeout), cookies=self.cookies) as session:

            # 1 URL 1 filter
            if not isinstance(urls, list) and not isinstance(filters, list):
                url_type = define_url(url=urls)
                if url_type == 'sql':
                    return await self.SQL.query_url(session=session, url=urls, format=format, filters=filters, filter_chunk_size=filter_chunk_size)
                elif url_type == 'card':
                    return await self.Card.query_card(session=session, url=urls, format=format, filters=filters, filter_chunk_size=filter_chunk_size)
                elif url_type == 'dataset':
                    return await self.Dataset.query_dataset(session=session, url=urls, format=format, filters=filters, filter_chunk_size=filter_chunk_size)

            # Make sure URL list and Filter list are the same length.
            else:
                # n URL 1 filters > OK
                if isinstance(urls, list) and not isinstance(filters, list):
                    filters = [filters for url in urls]
                # 1 URL n filters > OK
                elif not isinstance(urls, list) and isinstance(filters, list):
                    urls = [urls for f in filters]
                # n URL != filters > Raise
                elif len(urls) != len(filters):
                    raise ValueError('Filter list and URL list must be the same length. Supported 1 dict - 1 list, and 1 list - 1 list.')

                # Allocate URLs and Filters to functions.
                tasks = []
                for url, f in zip(urls, filters):
                    url_type = define_url(url=url)
                    if url_type == 'sql':
                        task = asyncio.create_task(self.SQL.query_url(session=session, url=url, format=format, filters=f, filter_chunk_size=filter_chunk_size))
                        task.url = url
                        task.filter = f
                        tasks.append(task)
                    elif url_type == 'card':
                        task = asyncio.create_task(self.Card.query_card(session=session, url=url, format=format, filters=f, filter_chunk_size=filter_chunk_size))
                        task.url = url
                        task.filter = f
                        tasks.append(task)
                    elif url_type == 'dataset':
                        task = asyncio.create_task(self.Dataset.query_dataset(session=session, url=url, format=format, filters=f, filter_chunk_size=filter_chunk_size))
                        task.url = url
                        task.filter = f
                        tasks.append(task)

                await asyncio.gather(*tasks, return_exceptions=True)

                record_results = [{'url': task.url, 'filter': task.filter, 'format': format, 'data': task.result()} for task in tasks]

                return record_results


    # Fetch data with retry
    async def export(self, session, url, form_data, format='json', column_sort=None):
        '''
        This function support fetch data with retry.

        :param session: aiohttp.ClientSession
        :param url: Export URL.
        :param form_data: Form data with dumped value.
        :param format: json, csv, xlsx.
        :param column_sort: Column sort order.
        :return:
        '''

        # Count for log
        self.query_count += 1
        query_number = self.query_count

        # Default headers of export API endpoint.
        headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
        if self.metabase_session:
            headers['X-Metabase-Session'] = self.metabase_session

        @retry(stop=stop_after_attempt(self.retry_attempts), reraise=True)
        async def handler():
            # Print log
            self.print_if_verbose(f'Querying {query_number}...')

            response = await session.post(url, headers=headers, data=form_data)

            # Raise if error: Connection, Timeout, Metabase server slowdown
            response.raise_for_status()

            # JSON
            if format == 'json':
                data = await response.json()
                if 'error' in data:
                    return raise_retry_errors(error=data['error'], retry_errors=self.retry_errors)
                elif column_sort:
                    data = [{col: record[col] for col in column_sort if col in record} for record in data]

            # XLSX, CSV: Success -> Content, Error -> JSON
            else:
                data = await response.read()
                if b'"error":' in data:
                    data = await response.json()
                    return raise_retry_errors(error=data['error'], retry_errors=self.retry_errors)

            return data

        # Call handler
        result = await handler()
        # Raise for user errors
        if isinstance(result, Exception):
            raise result
        else:
            # Print log then return
            self.print_if_verbose(f'Received data {query_number}')
            return result