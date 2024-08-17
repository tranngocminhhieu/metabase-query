import asyncio
import aiohttp
from .card import Card
from .dataset import Dataset
from .sql import SQL

import nest_asyncio
nest_asyncio.apply()

from tenacity import *
from .utils import raise_retry_errors, combine_results, define_url

class Metabase(object):
    def __init__(self, metabase_session, retry_errors=None, retry_attempts=3, limit_per_host=5, timeout=600, verbose=True, domain=None):
        self.metabase_session = metabase_session
        self.retry_errors = retry_errors
        self.retry_attempts = retry_attempts
        self.limit_per_host = limit_per_host
        self.timeout = timeout
        self.verbose = verbose
        self.domain = domain

        self.Card = Card(metabase=self)
        self.Dataset = Dataset(metabase=self)
        self.SQL = SQL(metabase=self)

    def print_if_verbose(self, *args):
        if self.verbose:
            print(*args)

    def query(self, urls, filters=None, formats='json'):
        pass

        is_url_list = isinstance(urls, list)
        is_param_list = isinstance(filters, list)
        is_format_list = isinstance(formats, list)

        # Người dùng điền nhiều format
            # Số lượng format = số lượng url hoặc = số lượng params -> OK
            # Lỗi

        # Người dùng điền 1 URL
            # Người dùng điền 0-1 params -> Run 1
            # Người dùng điền nhiều params -> Run nhiều

        # Người dùng điền nhiều URL
            # Người dùng điền 0 params -> Run nhiều
            # Người dùng điền 1 params -> Lỗi
            # Người dùng điền nhiều params
                # Số lượng params = Số lượng URL -> run nhiều
                # Số lượng params != Số lượng URL -> Lỗi




    def sql_query(self, sqls, databases, format='json'):
        result = asyncio.run(self.SQL.query_sql(sqls=sqls, databases=databases, format=format))
        return result



    async def handle_urls(self, urls, format='json', filters=None, filter_chunk_size=5000):

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.limit_per_host), timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:

            # 1 URL 1 filters
            if not isinstance(urls, list):
                url_type = define_url(url=urls)
                if url_type == 'sql':
                    if filters:
                        raise ValueError('Currently unsupported filters for SQL URL.')
                    else:
                        return await self.SQL.export_url(session=session, url=urls, format=format)
                elif not isinstance(filters, list):
                    if url_type == 'card':
                        return await self.Card.query_card(session=session, url=urls, format=format, filters=filters, filter_chunk_size=filter_chunk_size)
                    elif url_type == 'dataset':
                        return await self.Dataset.query_dataset(session=session, url=urls, format=format, filters=filters, filter_chunk_size=filter_chunk_size)
                # 1 URL n filters
                else:
                    tasks = []
                    for f in filters:
                        if url_type == 'card':
                            task = asyncio.create_task(self.Card.query_card(session=session, url=urls, format=format, filters=f, filter_chunk_size=filter_chunk_size))
                            tasks.append(task)
                        elif url_type == 'dataset':
                            task = asyncio.create_task(self.Dataset.query_dataset(session=session, url=urls, format=format, filters=f, filter_chunk_size=filter_chunk_size))
                            tasks.append(task)

                    results = await asyncio.gather(*tasks)
                    dict_results = [{'url': urls, 'filters': f, 'format': format, 'data': r} for f, r in zip(filters, results)]
                    return dict_results

            # n URL
            else:
                tasks = []
                for url in urls:
                    url_type = define_url(url=url)






    async def export(self, session, url, form_data, format='json', column_sort=None):
        format = format.lower()
        headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8', 'X-Metabase-Session': self.metabase_session}

        @retry(stop=stop_after_attempt(self.retry_attempts), reraise=True)
        async def handler():
            self.print_if_verbose('Querying')
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

        result = await handler()
        if isinstance(result, Exception):
            raise result
        else:
            return result