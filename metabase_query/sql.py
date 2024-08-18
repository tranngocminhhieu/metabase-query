from urllib import parse
import json
import base64
import re
import aiohttp
import asyncio

class SQL:
    def __init__(self, metabase):
        self.metabase = metabase

    async def export_url(self, session, url, format='json'):
        '''
        Export data for SQL URL.

        :param session: aiohttp.ClientSession.
        :param url: SQL URL.
        :param format: json, csv, xlsx.
        :return: One data.
        '''

        parse_result = parse.urlparse(url=url)
        domain = f"{parse_result.scheme}://{parse_result.netloc}"
        query = json.loads(base64.b64decode(parse_result.fragment))
        dataset_query = query['dataset_query']

        if query.get('parameters'):
            raise ValueError('Currently unsupported parameters for SQL.')

        form_data = {'query': json.dumps(dataset_query)}

        url = f"{domain}/api/dataset/{format}"

        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format)

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
                await asyncio.gather(*tasks)
                record_results = [{'sql': task.sql, 'database': task.database, 'data': task.result()} for task in tasks]
                return record_results