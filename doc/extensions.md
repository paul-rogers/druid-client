# Extensions

Druid provides a powerful extension mechanism: vendors and users can add 
functionality to all parts of Druid. Extensions can add user-visible features 
such as new endpoints  or new ways to process queries. `druid-client` provides a 
parallel extension mechanism to provide client-side features to match those 
introduced by Druid-side extenstions.

You can create an extension module that sits atop `druid-client`. In this case,
you just use the standard Python `import` mechanism to load your extension,
and you might pass the `druid-client` client to your module:

```python
import foo

myFoo = foo.FooBlaster(client)
```

There are times, however, when you want tighter integration. If your Druid provides
REST endpoints, you'd like those available from the `druid-client` client. To do
that, you define a `druid-client` extension.

The `druid-client` extension mechanism has several parts:

* *Discovery*: Allow `druid-client` to find your extension.
* *Patching*: `druid-client` adds methods to the `druid-client` classes to access your extension
* *Extension classes*: Offer additional services via extension-specific classes.

## Discovery

`druid-client` uses simplest of the [standard techniques](
https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/)
to find ("discover") extensions: if your extension is named with the
`druid_` prefix, then `druid-client` will find it. For example, if your Druid
extension is `foo-extension`, then the `druid-client` extension could be `druid_foo`.

To avoid confusion with a project that happens to start with `druid-`, `druid-client`
looks for a module named `register` within the project, and expects the module to
export a function named `register_client`. In your `__init__.py` file:
function also named `register`:

```python
def register_client(context):
    # Regiser your extension here
```

## Registration

The registration function provides a registration context which you use to integrate
your module with `druid-client` by setting various variables within the context:

* `summary`: The display name of your module. Defaults to the module name minus the
`druid-` prefix plus " extension".
* `client`: If your module provides client endpoints, set this variable to your
client *class* (not an *instance* of the class, but to the class itself). `druid-client`
will create an instance of the class, passing it the `druid-client` client.
* `roles`: If your extension provides a Druid service, set this to a map of service
name to the your extension class which acts as a client to that service.

The `client` class should provide operations which are available via the Broker
or Router, or are implemented by existing endpoints on those services. If your
extension calls methods on other Druid nodes, then register it as a cluster
extension instead. (TODO: define the cluster extension.) This split allows the
`druid-client` to work if the user only provides the Router or Broker URL.
(Accessing other services running in Docker or the cloud often requires additional
mapping information and is handled by the cluster class.)

The Imply extension is a good example: Imply provides additional client
endpoints, and provides an additional service.

**Note**: The above is just a start. Extension points will also be added for
working with queries and formatting output, among other tasks. Post an issue or pull
requset if you have a need not covered here.

## Patching

`druid-client` patches the client and cluster classes to add methods for your
extension. If you set the `client` variable above, then `druid-client` will create
a method on the client class called `foo_client` to return an instance of your
client class. `druid-client` creates a single instance per connection, which it
caches. If the user asks for your client multiple times within the same connection,
a single instance is created and cached.

Similarly, if you set th `roles` variable, then `druid-client` will create a method
on the cluster class for each of your services, called `<role>_client` Again, the
client is cached per connection.

## Extension Classes

TODO: revise this part.

Beyond direct API support, an extension should simply offer additional
classes and functions to do useful work. For example, if our "foo" extension
exists to monitor certain parts of the Druid cluster, we may offer an
entire set of classes to analyze the data. These would be imported by
the user in usual way. They can be connected to the `druid-client` client
in one of two ways. First, just pass a parameter:

```python
from foo import Analyzer

a = Analyzer(client)
```

Or, the extension can monkey-patch the `Cluster` class to allow:

```python
a = client.cluster().analyzer()
```