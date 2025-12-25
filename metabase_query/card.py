import asyncio
import copy
import json
import re
from urllib import parse

from .utils import split_list, combine_results, parse_filters


class Card:
    def __init__(self, metabase):
        self.metabase = metabase


    async def parse_card(self, session, url, filters=None):
        '''
        Parse a card to build URL and parameters.

        :param session: aiohttp.ClientSession.
        :param url: URL to parse.
        :param filters: Filters to add to parameters.
        :return: Card data as dict.
        '''
        # Print log
        self.metabase.parse_count += 1
        parse_number = self.metabase.parse_count
        self.metabase.print_if_verbose(f'Parsing URL and verifying Metabase Session {parse_number}')

        # Parse URL
        parse_result = parse.urlparse(url=url)
        domain = f"{parse_result.scheme}://{parse_result.netloc}"
        question = re.search(pattern=r'^/question/(\d*)(\-.*)?', string=parse_result.path).group(1)
        query = parse.parse_qs(parse_result.query)

        # Fetch card information
        headers = {'Content-Type': 'application/json'}
        if self.metabase.metabase_session:
            headers['X-Metabase-Session'] = self.metabase.metabase_session
        card_url = f'{domain}/api/card/{question}'
        response = await session.get(url=card_url, headers=headers)

        # Raise if error
        error_dict = {
            401: 'Session is not valid',
            404: 'Question is not exist, or you do not have permission',
        }

        if not response.ok:
            if response.status in error_dict:
                raise PermissionError(error_dict[response.status])
            else:
                response.raise_for_status()

        card_data = await response.json()

        # Find column sort
        result_metadata = card_data.get('result_metadata')
        if result_metadata:
            column_sort = [col['display_name'] for col in result_metadata]
        else:
            column_sort = None

        # Create parameters
        parameters = []
        card_parameters = card_data.get('parameters')
        template_tags = card_data['dataset_query'].get('native', {}).get('template-tags', {}) if card_data.get('dataset_query') else None

        # Priority for card_parameters
        if card_parameters:
            if filters:
                # Raise if filter slug is not valid.
                available_parameter_slugs = [p['slug'] for p in card_parameters]
                invalid_filters = set(filters) - set(available_parameter_slugs)
                if invalid_filters:
                    raise ValueError(f"The {', '.join(invalid_filters)} {'filters' if len(invalid_filters) > 2 else 'filter'} {'are' if len(invalid_filters) > 2 else 'is'} not available for this query. These are the available filters: {', '.join(available_parameter_slugs)}.")
                else:
                    # Save filter to query.
                    for filter in filters:
                        query[filter] = filters[filter]

            # Convert to dict for mapping
            needed_parameters = {p['slug']: p for p in card_parameters if p['slug'] in query}

            # Create parameters
            for q in query:
                param_type = needed_parameters[q]['type']
                param_target = needed_parameters[q]['target']
                param_value = query[q]
                if 'number' in param_type:
                    param_value = [float(i) for i in param_value]
                if 'date' in param_type:
                    param_value = param_value[0]
                parameter = {'type': param_type, 'value': param_value, 'target': param_target}
                parameters.append(parameter)

        elif template_tags:

            if filters:
                # Raise if filter slug is not valid.
                invalid_filters = set(filters) - set(template_tags)
                if invalid_filters:
                    raise ValueError(f"The {', '.join(invalid_filters)} {'filters' if len(invalid_filters) > 2 else 'filter'} {'are' if len(invalid_filters) > 2 else 'is'} not available for this query. These are the available filters: {', '.join(template_tags)}.")
                else:
                    # Save filter to query.
                    for filter in filters:
                        query[filter] = filters[filter]

            not_dimension_tag_type_to_param_type = {
                'date': 'date/single',
                'number': 'number/=',
                'text': 'category'
            }

            # Create parameters
            for q in query:
                tag = template_tags[q]
                tag_type = tag['type']

                if tag_type == 'dimension':
                    param_type = tag['widget-type']
                else:
                    param_type = not_dimension_tag_type_to_param_type[tag_type]

                param_target = [tag_type if tag_type == 'dimension' else 'variable', ['template-tag', q]]

                param_value = query[q]
                if 'number' in param_type:
                    param_value = [float(i) for i in param_value]
                if 'date' in param_type:
                    param_value = param_value[0]

                parameter = {'type': param_type, 'value': param_value, 'target': param_target}
                parameters.append(parameter)

        elif not card_parameters and not template_tags and query:
            raise LookupError('Can not build parameters payload for this question, please re-save your question and try again.')

        data = {
            'domain': domain,
            'question': question,
            'parameters': parameters,
            'column_sort': column_sort
        }

        return data


    async def export_card(self, session, card_data, format='json'):
        url = f"{card_data['domain']}/api/card/{card_data['question']}/query/{format}"
        form_data = {'parameters': json.dumps(card_data['parameters'])}
        return await self.metabase.export(session=session, url=url, form_data=form_data, format=format, column_sort=card_data['column_sort'])


    async def query_card(self, session, url, format='json', filters=None, filter_chunk_size=5000):
        '''
        Send one request or multiple requests to get data from Metabase.

        :param session: aiohttp.ClientSession
        :param url: URL from browser.
        :param format: json, csv, xlsx.
        :param filters: A dict.
        :param filter_chunk_size: If you have a bulk value filter, the package will splits your values into chunks to send the requests, and then concat the results into a single data.
        :return: Combined data.
        '''


        filters, max_filter_key, max_filter_value_count = parse_filters(filters)

        card_data = await self.parse_card(session=session, url=url, filters=filters)

        # Send one request if there are no filter has values > filter_chunk_size
        if max_filter_value_count <= filter_chunk_size:
            return await self.export_card(session=session, card_data=card_data, format=format)
        else:
            if format not in ['json', 'csv']:
                raise ValueError(f'Package only supports JSON and CSV formats with bulk filter values due to data combining limitations. Your {max_filter_key} filter is over filter_chunk_size {max_filter_value_count}/{filter_chunk_size}.')

            # Slit values to chunks > Create a list of card_data
            value_list = split_list(input_list=filters[max_filter_key], chunk_size=filter_chunk_size)
            card_data_list = []
            for value in value_list:
                new_card_data = copy.deepcopy(card_data)
                for parameter in new_card_data['parameters']:
                    if parameter['target'][-1][-1] == max_filter_key:
                        parameter['value'] = value

                card_data_list.append(new_card_data)

            # Send requests to get data in bulk.
            tasks = []
            for c in card_data_list:
                task = asyncio.create_task(self.export_card(session=session, card_data=c, format=format))
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return combine_results(results=results, format=format, verbose=self.metabase.verbose)