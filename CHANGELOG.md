# 1.0.1
- Add `return_exceptions` to `asyncio.gather`.
- Auto apply nest_asyncio for Notebook.

# 1.0.2
- Rename function `sql_query` to `sql`.

# 1.0.0
1. Get question data in any data format provided by Metabase (JSON, CSV, XLSX).
2. Input question URL and Metabase Session. No need to provide parameters payload.
3. JSON results have the same column sort order as the browser.
4. Automatically check if Metabase session is available.
5. Easy to filter data with a simple dict.
6. Allow retry if an error occurs due to server slowdown.
7. Allow entering multiple filter values in bulk.
8. Support both saved questions and unsaved questions.
9. Support SQL query.