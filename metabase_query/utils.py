import re
from urllib import parse
import json
import base64
from http.cookies import SimpleCookie

def raise_retry_errors(error, retry_errors):
    if not retry_errors:
        raise Exception(f"Any error: {error}")
    elif re.search(pattern='|'.join(retry_errors), string=error, flags=re.IGNORECASE):
        raise Exception(f"Retry error: {error}")
    else:
        return Exception(error)


def split_list(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]


def combine_results(results, format='json', verbose=True):
    format = format.lower()
    if format not in ['json', 'csv']:
        raise ValueError('This function supports JSON and CSV due to data combining limitations.')

    if [r for r in results if isinstance(r, Exception)] and verbose:
        print('Some requests failed because the retry count was exceeded. However, you still received data from successful requests.')

    success_results = [r for r in results if not isinstance(r, Exception)]

    if format == 'json':
        combined_data = sum(success_results, [])
    elif format == 'csv':
        combined_data = []
        for i, data in enumerate(success_results):
            lines = data.decode('utf-8').splitlines()
            if i == 0:
                combined_data.extend(lines)
            else:
                combined_data.extend(lines[1:])
        combined_data = "\n".join(combined_data).encode('utf-8')

    return combined_data


def define_url(url):
    parse_result = parse.urlparse(url=url)
    if re.search(pattern='^/question/(\d*)(\-.*)?', string=parse_result.path):
        return 'card'
    else:
        query = json.loads(base64.b64decode(parse_result.fragment))
        dataset_query = query['dataset_query']
        if dataset_query['type'] == 'native':
            return 'sql'
        else:
            return 'dataset'


def parse_filters(filters):
    if filters:
        # Rename keys
        filters = {str(f).lower().replace(' ', '_'): filters[f] for f in filters}
        # Convert value to list
        for filter in filters:
            if not isinstance(filters[filter], list):
                filters[filter] = [filters[filter]]
        # Find max key, value count
        max_filter_key = max(filters, key=lambda k: len(filters[k]))
        max_filter_value_count = len(filters[max_filter_key])
    else:
        max_filter_key = None
        max_filter_value_count = 0

    data = (filters, max_filter_key, max_filter_value_count)

    return data


def parse_raw_cookies(file):
    '''
    How to get cookie_file?
    - Use Cookie-Editor extension on Chrome to get the raw cookie and save it to a text file, for example: cookie.txt
    - Extension link: https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm

    :param file: cookies.txt
    :return: cookies as dict
    '''
    with open(file, 'r') as f:
        raw_cookie = f.read()
    try:
        cookie = json.loads(raw_cookie)
        cookies = {item['name']:item['value'] for item in cookie}
    except:
        cookie = SimpleCookie()
        cookie.load(raw_cookie)
        cookies = {key:value.value for key,value in cookie.items()}
    return cookies