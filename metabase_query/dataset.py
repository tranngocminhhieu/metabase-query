import json
import base64
from urllib import parse
from .utils import split_list, combine_results, parse_filters
import asyncio
import copy


class Dataset:
    def __init__(self, metabase):
        self.metabase = metabase


    async def parse_dataset(self, session, url, filters=None):
        '''
        Parse a dataset to build URL and query filter.

        :param session: aiohttp.ClientSession.
        :param url: URL to parse.
        :param filters: Filters to add to query filter.
        :return: Dataset data as dict.
        '''
        # Print log
        self.metabase.parse_count += 1
        parse_number = self.metabase.parse_count
        self.metabase.print_if_verbose(f'Parsing URL and verifying Metabase Session {parse_number}')

        # Parse URL
        parse_result = parse.urlparse(url=url)
        domain = f"{parse_result.scheme}://{parse_result.netloc}"
        query = json.loads(base64.b64decode(parse_result.fragment))
        dataset_query = query['dataset_query']  # For export
        source_table = dataset_query['query']['source-table']  # For parse

        # Fetch table information
        headers = {'Content-Type': 'application/json'}
        if self.metabase.metabase_session:
            headers['X-Metabase-Session'] = self.metabase.metabase_session
        url = f'{domain}/api/table/{source_table}/query_metadata'
        response = await session.get(url=url, headers=headers)

        # Raise if error
        error_dict = {
            401: 'Session is not valid.',
            404: 'Table does not exist or you do not have permission.',
        }

        if not response.ok:
            if response.status in error_dict:
                raise PermissionError(error_dict[response.status])
            else:
                response.raise_for_status()

        table_data = await response.json()

        # Find column sort
        query_fields = dataset_query['query'].get('fields') # When we drag columns on browser.
        fields = table_data.get('fields')

        # Priority for query_fields
        if query_fields:
            query_field_ids = [f[1] for f in query_fields]
            field_display_names = {f['id']:f['display_name'] for f in fields}
            column_sort = [field_display_names[i] for i in query_field_ids]
        else:
            column_sort = [f['display_name'] for f in fields]

        if filters:
            # Raise if filter slug is not valid.
            available_field_names = [f['name'] for f in fields]
            invalid_filters = set(filters) - set(available_field_names)
            if invalid_filters:
                raise ValueError(f"The {', '.join(invalid_filters)} {'filters' if len(invalid_filters) > 2 else 'filter'} {'are' if len(invalid_filters) > 2 else 'is'} not available for this table. These are the available filters: {', '.join(available_field_names)}.")
            else:
                # Create query filter
                query_filters = []
                filter_ids = []
                for filter in filters:
                    field_id = [f['id'] for f in fields if f['name'] == filter][0]
                    query_filter = ["=",["field",field_id,None]] + filters[filter]
                    query_filters.append(query_filter)
                    filter_ids.append(field_id)

                if 'filter' not in dataset_query:
                    dataset_query['query']['filter'] = ['and'] + query_filters
                else:
                    dataset_query['query']['filter'] = ['and'] + query_filters + [f for f in dataset_query['query']['filter'][1:] if f[1][1] not in filter_ids]


        data = {
            'domain': domain,
            'dataset_query': dataset_query,
            'column_sort': column_sort,
            'fields': [{'name': f['name'], 'id': f['id'], 'display_name': f['display_name']} for f in fields]
        }
        return data

    async def export_dataset(self, session, dataset_data, format='json'):
        url = f"{dataset_data['domain']}/api/dataset/{format}"
        form_data = {'query': json.dumps(dataset_data['dataset_query'])}
        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format, column_sort=dataset_data['column_sort'])


    async def query_dataset(self, session, url, format='json', filters=None, filter_chunk_size=5000):
        '''
        Send one request or multiple requests to get data from Metabase.

        :param session: aiohttp.ClientSession.
        :param url: URL to query.
        :param format: json, csv, xlsx.
        :param filters: A dict.
        :param filter_chunk_size: If you have a bulk value filter, the package will splits your values into chunks to send the requests, and then concat the results into a single data.
        :return: Combined data.
        '''

        filters, max_filter_key, max_filter_value_count = parse_filters(filters)

        dataset_data = await self.parse_dataset(session=session, url=url, filters=filters)

        # Send one request if there are no filter has values > filter_chunk_size
        if max_filter_value_count <= filter_chunk_size:
            return await self.export_dataset(session=session, dataset_data=dataset_data, format=format)

        else:
            if format not in ['json', 'csv']:
                raise ValueError(f'Package only supports JSON and CSV formats with bulk filter values due to data combining limitations. Your {max_filter_key} filter is over filter_chunk_size {max_filter_value_count}/{filter_chunk_size}.')

            # Slit values to chunks > Create a list of dataset_data
            value_list = split_list(input_list=filters[max_filter_key], chunk_size=filter_chunk_size)

            # Get field ID to filter in loop
            field_id = [f['id'] for f in dataset_data['fields'] if f['name'] == max_filter_key][0]
            dataset_data.pop('fields')

            # List of dataset_data
            dataset_data_list = []
            for value in value_list:
                query_filter = ["=", ["field", field_id, None]] + value
                new_dataset_data = copy.deepcopy(dataset_data)
                query_filters = new_dataset_data['dataset_query']['query']['filter']
                new_dataset_data['dataset_query']['query']['filter'] = ['and'] + [query_filter] + [f for f in query_filters[1:] if f[1][1] != field_id]
                dataset_data_list.append(new_dataset_data)

            # Send requests to get data in bulk.
            tasks = []
            for d in dataset_data_list:
                task = asyncio.create_task(self.export_dataset(session=session, dataset_data=d, format=format))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return combine_results(results=results, format=format, verbose=self.metabase.verbose)