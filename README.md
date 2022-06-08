# druid-client

`druid-client` is a Python library to interact with all aspects of your
[Apache Druid](https://druid.apache.org/) or [Imply](https://imply.io/) cluster. 
`druid-client` picks up where the venerable [pydruid](https://github.com/druid-io/pydruid) lbrary 
left off to include full SQL support and support for many of of Druid APIs. `druid-client`
also provides tools to analze tables, plan your cluster and more. `druid-client` is usable 
in any Python environment, but is optimized for use in Jupyter, providing a complete interactive
environment which complements the UI-based Druid console.

An extension, `druid-imply`, adds support for Imply-specific extensions to Apache Druid.

## Install

TODO: Not yet available.

To install:

```bash
pip install druid_client
```

Dependencies:

* `requests`
* `pandas` (Optional)
* `pydruid` (Optional)

## Documentation

Documentation: [available on GitHub](need link).

## Getting Started

To use `druid-client`, you must first import the library, then connect to your cluster by providing the URL to your Router or Broker instance.

```python
import druid_client as dcl

client = dcl.connect("http:\\localhost:8888")
```

## Querying

The original [`pydruid`](https://pythonhosted.org/pydruid/) library revolves around Druid 
"native" queries. If you have `pydruid` installed, you can use it as follows:

```python
native_client = client.pydruid()
# Run a query
```

See the [`pydruid` examples](https://github.com/druid-io/pydruid] for mor information.

Most new applications now use SQL. To obtain the results of a SQL query against the example Wikipedia table (datasource) in a "raw" form:

```python
sql = '''
SELECT
  channel,
  COUNT(*) AS "count"
FROM wikipedia
GROUP BY channel
ORDER BY COUNT(*) DESC
LIMIT 5
'''
client.sql(sql)
```

Gives:

```text
[{'channel': '#en.wikipedia', 'count': 6650},
 {'channel': '#sh.wikipedia', 'count': 3969},
 {'channel': '#sv.wikipedia', 'count': 1867},
 {'channel': '#ceb.wikipedia', 'count': 1808},
 {'channel': '#de.wikipedia', 'count': 1357}]
```

The raw results are handy when Python code consumes the results, or for a quick check. For other visualizations, and to exercise more control over your queries, you can use the query request and response objects. A string is a request that is assumed to be SQL:

```python
result = client.sql_query(sql)
```

The result can provide the raw results, and it can format the results in various ways. As a simple text table:

```python
result.show()
```

Gives:

```text
channel        count
#en.wikipedia   6650
#sh.wikipedia   3969
#sv.wikipedia   1867
#ceb.wikipedia  1808
#de.wikipedia   1357
```

Within Jupyter, the results can be formatted as an HTML table:

```python
client.show_as_html()
results.show()
```

<img src="doc/imgs/query-html.jpg">Example query table</img>

If [Pandas](need link) is installed, export data a Pandas data frame:

```python
results.data_frame()
```

&lt;Example&gt;

The request object can also be a native query (see below), can include Druid "context options", can provide parameters and more. See `SQLRequest` in the documentation. (Add link.)

## Full API Support

`druid-client` provides a wrapper for most of the [Druid REST API](need link). Operations are organized around the *service* which Druid provides. Each service runs on a node and provides a REST endpoint described by the host name and a port. We used the service endpoint for a local router above: `http://localhost:8888`. The *cluster* concept allows accesses to the services within your cluster:

```python
cluster = client.cluster()
coord = cluster.coord()
```

See the documentation for other cases.

## Native Queries

`druid-client` carries over native query support from `pydruid`. While you may find SQL support to be more convenient for most tasks, the native query support is still helpful if you are a Druid developer who needs to experiment with, test or exercise native queries.

&lt;Example&gt;

## Cluster Management Tools

With access to SQL, the Druid API, and native queries, we now have what we need to explore and manage a Druid cluster. For example, we can explore the services within our cluster:

&lt;Example&gt;

Or, we can obtain information about a table:

&lt;Example&gt;

We can even do planning, such as estimating the future size of a table given a subset of data and an expected retention period:

&lt;Example&gt;

## Dependencies

`pyddruid-clientuid2` depends on a number of external libraries:

* `requests` for the HTTP client

Some dependencies are optional:

* `pandas`

## Extensions

`druid-client` is designed for extensibility. You can add support for custom services, custom managment tools, custom output formats, and more. See the [Documentation](need link) for details.

## Helpful Additions

If you are setting up or debugging a Druid cluster, several addtional Python libraries
are handy. This is especially true if you work with the Druid integration tests and
need to understand the state of the cluster.

* Use [Kazoo](https://pypi.org/project/kazoo/) to poke around inside Zookeeper.
* Use [MySQL Connector/Python]
  (https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html)
  to explore the Druid metadata storage database.

Kazoo example:

```Python
from kazoo.client import KazooClient
zk = KazooClient(hosts='localhost:2181')
zk.start()
zk.get_children('/druid/internal-discovery')
zk.stop()
```

Displays:

```text
['INDEXER',
 'PEON',
 'BROKER',
 'OVERLORD',
 'MIDDLE_MANAGER',
 'COORDINATOR',
 'HISTORICAL',
 'ROUTER',
 'custom-node-role']
```

MySQL example:

```python
def db_query(sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    for item in cursor:
        print(item)
    cursor.close()

import mysql.connector
conn = mysql.connector.connect(user="druid", password='diurd',
                              host='localhost',
                              database='druid')
db_query('show tables')
```

Displays:

```text
('druid_audit',)
('druid_config',)
('druid_dataSource',)
('druid_pendingSegments',)
('druid_rules',)
('druid_segments',)
('druid_supervisors',)
('druid_tasklocks',)
('druid_tasklogs',)
('druid_tasks',)
```

## Related Projects

`druid-client` is not the only Druid library available. Others include:

* [`pydruid`](https://pypi.org/project/pydruid/) - Official Python library of the Apache Druid
project. Focused on building and running native queries.
* [`druidpy`](https://pypi.org/project/druidpy/) - Provides simple access ot the most
commonly used Druid APIs.
* [`druid-query`](https://pypi.org/project/druid-query/) - Supports SQL and native queries,
including async support.

## Contributions

Contributions are welcome and encouraged! As you use `druid-client` in your own project, notice what might be added to make your life easier. You can contribute an extension (such as for a management tool you find handy), or expand core functionality (perhaps to support APIs not fully supported in the present version.) See the [Documentation](need link) for details.
