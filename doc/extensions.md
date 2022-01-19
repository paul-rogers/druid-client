# Extensions

Druid provides a powerful extension mechanism: vendors and users can add 
functionality to all parts of Druid. Extensions can add user-visible features 
such as new endpoints  or new ways to process queries. `pydruid2` provides a 
parallel extension mechanism to provide client-side features to match those 
introduced by Druid-side extenstions.

The `pydruid2` extension mechanism has several parts:

* *Discovery*: Allow `pydruid2` to find your extension.
* *Patching*: Add methods to the `pydruid2` classes to access your extension
* *Extension classes*: Offer additional services via extension-specific classes.

## Discovery

`pydruid2` uses simplest of the [standard techniques](
https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/)
to find ("discover") extensions: if your extension is named with the
`pydruid2_` prefix, then `pydruid2` will find it. For example, if your Druid
extension is `foo-extension`, then the `pydruid2` extension could be `pydruid2_foo`.


## Patching

`pydruid2` uses [monkey patching]
(https://www.pythonforthelab.com/blog/monkey-patching-and-its-consequences/), 
which can lead to unstable code if used wrong, but we'll use it with rigor here.

Suppose a Druid extension, `foo-exension` adds a new endpoint to the router
called `/foo` and we wish to make that available from the `pydruid2` client
as follows:


```python
value = client.foo()
```

Provide a top-level `register(pd2)` method in `__init__.py`.
Within that function. patch the available classes.

```python
def extended_foo(self):
    return self.get_json('/foo')

def register(pd2):
    pd2.Client.foo = extended_foo
```

You will typically put the actual implementions in some other file:

`__init__.py`:

```python
import foo

def register(pd2):
    foo.register(pd2)
```

`foo.py` then contains the monkey-patching code from above.

### Naming Guidelines

Druid users may use modules from different projects or vendors. When choosing a
method name, make it unique enough that it is unlikely to collide with a
Druid-added feature or with other extensions. That is use, say, `fooHealth`
rather than just `health` for an endpoint which checks the health of the
"foo" extension.

## Extension Classes

Beyond direct API support, an extension should simply offer additional
classes and functions to do useful work. For example, if our "foo" extension
exists to monitor certain parts of the Druid cluster, we may offer an
entire set of classes to analyze the data. These would be imported by
the user in usual way. They can be connected to the `pydruid2` client
in one of two ways. First, just pass a parameter:

```python
from foo import Analyzer

a = Analyzer(client)
```

Or, the extension can monkey-patch the `Cluster` class to allow:

```python
a = client.cluster().analyzer()
```