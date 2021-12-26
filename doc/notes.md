# Working Notes

## Model

In the original code, the model layer built up a descrition of tables,
and did extrapolations, sizing and other tasks. The classes end up rather
complex.

For this version, perhaps:

* The model layer has different classes for different concepts.
* A single "uber" class gathers the various parts.
* Extrapolations reside in separate "app" modules.

This separates the concrete (how the table works today) from the speculative
(what might happen if you did thus-and-so.)

## Reports

In the original code, the reports layer handled display formatting, but also
acted as an abstraction on the model: the model was an internal implementation
detail for the reports. This made the reports API rather large and brittle.

In `pydruid2`, perhaps revise this a bit. At the cost of a bit more user
complexity, allow the model to be primary object. The reports are just a
way to visualize the model.

```python
model = cluster.model()
schema = model.table_schema('wikipedia')
schema.load_ingest('wikispec')

import pydruid2.reports as rpts

rpts.show(schema)
```

Or, maybe define display "back ends":

```python
rpts.dislay_as('html')
schema.show()
```

In this case, the display logic would be in a subclass and the model
`show()` method would delegate to `reports` to chose the right format.
This is a vastly simplified form of how `matplotlib` works.
