# pydruid2

`pydruid2` is a Python library to interact with all aspects of your Druid cluster. `pydruid2` is the next generation of the venerable [pydruid](https://github.com/druid-io/pydruid) lbrary, expanded with full SQL support and support for the entire set of Druid APIs. pydruid2 provides tools to analze tables, plan your cluster and more. `pydruid2` is usable in any Python environment, but is optimized for use in Jupyter, providing a complete interactive environent which complements the UI-based Druid console.

## Install

To install:

```bash
pip install pydruid2
```

## Documentation

Documentation: [available on GitHub](need link).

## Getting Started

To use pydruid2, you must first import the library, then connect to your cluster by providing the URL to your Router or Broker instance.

```python
import pydruid2

client = pydruid2.connect("http:\\localhost:8888")
```

## Querying

The original `pydruid` library revolves around Druid "native" queries. Native query support is carried over in `pydruid2`. Most new applications now use SQL. To obtain the results of a SQL query agains the example Wikipedia table (datasource) in a "raw" form:

```python
sql = "SELECT * FROM wikipedia LIMIT 10"
client.sql(sql)
```

Gives:

```text
# results
```

The raw results are handy when Python code consumes the results, or for a quick check. For other visualizations, and to exercise more control over your queries, you can use the query request and response objects. A string is a request that is assumed to be SQL:

```python
result = client.query(sql)
```

The result can provide the raw results, and it can format the results in various ways. As a simple text table:

```python
result.text_table()
```

Gives:

```text
# results
```

Within Jupyter, the results can be formatted as an HTML table:

```python
results.table()
```

&lt;Example&gt;

If [Pandas](need link) is installed, export data a Pandas data frame:

```python
results.data_frame()
```

&lt;Example&gt;

The request object can also be a native query (see below), can include Druid "context options", can provide parameters and more. See `SQLRequest` in the documentation. (Add link.)

## Full API Support

`pydruid2` provides a wrapper for most of the [Druid REST API](need link). Operations are organized around the *service* which Druid provides. Each service runs on a node and provides a REST endpoint described by the host name and a port. We used the service endpoint for a local router above: `http://localhost:8888`. The *cluster* concept allows accesses to the services within your cluster:

```python
cluster = client.cluster()
coord = cluster.coord()
```

See the documentation for other cases.

## Native Queries

`pydruid2` carries over native query support from `pydruid`. While you may find SQL support to be more convenient for most tasks, the native query support is still helpful if you are a Druid developer who needs to experiment with, test or exercise native queries.

&lt;Example&gt;

## Cluster Management Tools

With access to SQL, the Druid API, and native queries, we now have what we need to explore and manage a Druid cluster. For example, we can explore the services within our cluster:

&lt;Example&gt;

Or, we can obtain information about a table:

&lt;Example&gt;

We can even do planning, such as estimating the future size of a table given a subset of data and an expected retention period:

&lt;Example&gt;

## Dependencies

`pydruid2` depends on a number of external libraries:

* `requests` for the HTTP client

Some dependencies are optional:

* `pandas`

## Extensions

`pydruid2` is designed for extensibility. You can add support for custom services, custom managment tools, custom output formats, and more. See the [Documentation](need link) for details.

## Contributions

Contributions are welcome and encouraged! As you use `pydruid2` in your own project, notice what might be added to make your life easier. You can contribute an extension (such as for a management tool you find handy), or expand core functionality (perhaps to support APIs not fully supported in the present version.) See the [Documentation](need link) for details.