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
        format = format.lower()

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
        format = format.lower()

        if not self.metabase.domain:
            raise AttributeError('Please provide a domain for Metabase object to use SQL query.')

        if not re.search(pattern='https?://', string=self.metabase.domain):
            domain = f"https://{self.metabase.domain}"
        else:
            domain = self.metabase.domain

        found_database = re.search(pattern='(\d*)(\-.*)?', string=str(database))
        if not found_database:
            raise ValueError('Please input a valid database. Open your database on the browser and then copy the database slug or the database ID.')
        else:
            database_id = int(found_database.group(1))

        query = {
            "database": database_id,
            "native": {"query": sql},
            "type": "native",
        }

        url = f"{domain}/api/dataset/{format}"

        form_data = {'query': json.dumps(query)}

        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format)


    async def query_sql(self, sqls, databases, format='json'):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.metabase.limit_per_host), timeout=aiohttp.ClientTimeout(total=self.metabase.timeout)) as session:
            if not isinstance(sqls, list):
                if isinstance(databases, list):
                    if len(databases) > 2:
                        raise ValueError('Please input one database for one SQL.')
                    databases = databases[0]
                return await self.export_sql(session=session, sql=sqls, database=databases, format=format)
            else:
                if not isinstance(databases, list):
                    databases = [databases for sql in sqls]
                elif len(databases) != len(sqls):
                    raise ValueError('Databases must have the same length as the SQLs. Please input one database or a valid database list.')

                tasks = []
                for sql, db in zip(sqls, databases):
                    task = asyncio.create_task(self.export_sql(session=session, sql=sql, database=db, format=format))
                    tasks.append(task)
                results = await asyncio.gather(*tasks)

                dict_results = [{'sql': s, 'data': r} for s, r in zip(sqls, results)]
                return dict_results
