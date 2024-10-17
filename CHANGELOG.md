# 1.0.6
- Fix error with saved queries that does not have filter.

# 1.0.5
- Optimize code for reusable.
- Fix input parameter of SQL URL.

# 1.0.4
- Fix missing input filter_chunk_size for SQL URL.

# 1.0.3
- Add more package to install_requires.
- SQL URL support filters and parameters.

# 1.0.2
- Rename function `sql_query` to `sql`.

# 1.0.1
- Add `return_exceptions` to `asyncio.gather`.
- Auto apply nest_asyncio for Notebook.


# 1.0.0
- **Flexible Data Retrieval**: Retrieve data in any format supported by Metabase, including JSON, CSV, and XLSX.
- **Simple Integration**: Execute queries by simply inputting the question URL and Metabase session—no need to manually provide parameters.
- **Consistent Results**: JSON results maintain the same column order as displayed in the Metabase UI.
- **Session Management**: Automatically checks the availability of the Metabase session.
- **Effortless Filtering**: Easily apply filters to your queries using simple dictionaries.
- **Error Handling**: Supports automatic retries in case of server errors or slowdowns.
- **Bulk Filter Support**: Allows entering multiple filter values in a single request.
- **Query Versatility**: Supports both saved and unsaved questions, as well as SQL queries.
- **Utilize asynchronous libraries**: Send multiple requests concurrently to retrieve data efficiently.
