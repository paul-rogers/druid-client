# Working with Tables

Druid uses the awkward term "data source" to refer to data stored within Druid. Here we use the simpler term *table*. A table in Druid has a schema, and ingest specification and a large amount of additional metadata. Some of the metadata resides in system tables, others is available from REST APIs, more is in various specificatins, and some requires a native query. To simplify the task of working with tables, `pydruid2` provides the `Table` class which wraps all the various APIs required.

You obtain a table from the client:

```python
table = client.table('wikipedia')
```

You can then explore the table:

```python
table.name()

>>> 'wikipedia'

table.path()

>>> 'druid.wikipedia'

len(table.segments())

>>> 20

table.column_names()

>>> ['...']
```

There are operations to review segments, retention rules, ingest rules and much more. For visual inspection, such as in Jupyter, you can use the visualization wrapper:

```python
rpts = table.reports()
rpts.columns()
```

Produces an HTML table that details the columns:

&lt;Example&gt;

## Descriptive Metadata

When working with a table, expecially one with many columns, it can be helpful to provide additional column descriptions. It would be great if Druid maintained that information. But, until that occurs, you can add the descriptions for use in `pydruid2`.

&lt;Example&gt;
