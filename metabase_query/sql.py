import copy
from urllib import parse
import json
import base64
import re
import aiohttp
import asyncio
from .utils import split_list, combine_results, parse_filters

class SQL:
    def __init__(self, metabase):
        self.metabase = metabase


    async def parse_url(self, url, filters=None):
        parse_result = parse.urlparse(url=url)
        domain = f"{parse_result.scheme}://{parse_result.netloc}"
        fragment = json.loads(base64.b64decode(parse_result.fragment))
        dataset_query = fragment['dataset_query']
        query = parse.parse_qs(parse_result.query)
        parameters = fragment['parameters']

        # Prepare Form data (dataset_query)
        if filters:
            # Raise if filter slug is not valid.
            available_parameter_slugs = [p['slug'] for p in parameters]
            invalid_filters = set(filters) - set(available_parameter_slugs)
            if invalid_filters:
                raise ValueError(f"The {', '.join(invalid_filters)} {'filters' if len(invalid_filters) > 2 else 'filter'} {'are' if len(invalid_filters) > 2 else 'is'} not available for this query. These are the available filters: {', '.join(available_parameter_slugs)}.")
            else:
                # Save filter to query.
                for filter in filters:
                    query[filter] = filters[filter]

        for p in parameters:
            if p['slug'] in query:
                param_value = query[p['slug']]
                if 'number' in p['type']:
                    param_value = [float(i) for i in param_value]
                elif 'date' in p['type']:
                    param_value = param_value[0]
                p['value'] = param_value

        dataset_query['parameters'] = parameters

        data = {
            'domain': domain,
            'dataset_query': dataset_query
        }

        return data


    async def export_url(self, session, url_data, format='json'):
        url = f"{url_data['domain']}/api/dataset/{format}"
        form_data = {'query': json.dumps(url_data['dataset_query'])}
        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format)


    async def query_url(self, session, url, format='json', filters=None, filter_chunk_size=5000):
        '''
        Export data for SQL URL.

        :param session: aiohttp.ClientSession.
        :param url: SQL URL.
        :param format: json, csv, xlsx.
        :return: One data.
        '''

        filters, max_filter_key, max_filter_value_count = parse_filters(filters)

        url_data = await self.parse_url(url=url, filters=filters)

        # Send one request if there are no filter has values > filter_chunk_size
        if max_filter_value_count <= filter_chunk_size:
            return await self.export_url(session=session, url_data=url_data, format=format)

        else:
            if format not in ['json', 'csv']:
                raise ValueError(f'Package only supports JSON and CSV formats with bulk filter values due to data combining limitations. Your {max_filter_key} filter is over filter_chunk_size {max_filter_value_count}/{filter_chunk_size}.')

            # Slit values to chunks > Create a list of card_data
            value_list = split_list(input_list=filters[max_filter_key], chunk_size=filter_chunk_size)

            url_data_list = []
            for value in value_list:
                new_url_data = copy.deepcopy(url_data)
                for parameter in new_url_data['dataset_query']['parameters']:
                    if parameter['target'][-1][-1] == max_filter_key:
                        parameter['value'] = value
                url_data_list.append(new_url_data)

            tasks = []
            for u in url_data_list:
                task = asyncio.create_task(self.export_url(session=session, url_data=u, format=format))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return combine_results(results=results, format=format, verbose=self.metabase.verbose)


    async def export_sql(self, session, sql, database, format='json'):
        '''
        Export data for SQL query.

        :param session: aiohttp.ClientSession.
        :param sql: SQL query.
        :param database: One database ID. Look at the slug on the browser.
        :param format: json, csv, xlsx.
        :return: One data.
        '''

        # Prepare domain
        if not self.metabase.domain:
            raise AttributeError('Please provide a domain for Metabase object to use SQL query.')

        if not re.search(pattern='https?://', string=self.metabase.domain):
            domain = f"https://{self.metabase.domain}"
        else:
            domain = self.metabase.domain

        # Prepare database ID
        found_database = re.search(pattern='(\d*)(\-.*)?', string=str(database))
        if not found_database:
            raise ValueError('Please input a valid database. Open your database on the browser and then copy the database slug or the database ID.')
        else:
            database_id = int(found_database.group(1))

        # Create form data
        query = {
            "database": database_id,
            "native": {"query": sql},
            "type": "native",
        }
        form_data = {'query': json.dumps(query)}

        url = f"{domain}/api/dataset/{format}"

        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format)


    async def query_sql(self, sqls, databases, format='json'):
        '''
        Send one request or multiple requests with SQL to get data from Metabase.

        :param sqls: A SQL string or list of SQL.
        :param databases: One database ID or a list or database IDs follow SQL list. Look at the database slug on the browser.
        :param format: json, csv, xlsx.
        :return: One data or a list of data.
        '''
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.metabase.limit_per_host), timeout=aiohttp.ClientTimeout(total=self.metabase.timeout)) as session:

            # 1 SQL, 1 database
            if not isinstance(sqls, list) and not isinstance(databases, list):
                return await self.export_sql(session=session, sql=sqls, database=databases, format=format)

            else:
                # 1 SQL, n database > Raise
                if not isinstance(sqls, list) and isinstance(databases, list):
                    raise ValueError('Please provide one database ID as string for one SQL query.')
                # n SQL, 1 database > OK
                elif isinstance(sqls, list) and not isinstance(databases, list):
                    databases = [databases for sql in sqls]
                # n SQL != n database > Raise
                elif len(sqls) != len(databases):
                    raise ValueError('Database list and SQL list must be the same length. Supported 1 SQL - 1 database, n SQL - 1 database, and n SQL - n database')

                tasks = []
                for sql, db in zip(sqls, databases):
                    task = asyncio.create_task(self.export_sql(session=session, sql=sql, database=db, format=format))
                    task.sql = sql
                    task.database = db
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)
                record_results = [{'sql': task.sql, 'database': task.database, 'data': task.result()} for task in tasks]
                return record_results