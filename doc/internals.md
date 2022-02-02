# `druid-client` Internal Design

`druid-client` is divided into a set of modules. The lowest levels provide basic 
API support, while higher levels provide additional abstractions which build
on the basics. An application can import only those parts which it needs.

## Root

The root `druid-client` package contains only one function: to create a connection.
All other operations reside within the other modules.

## Client

The `client` module provides connectivity to the Druid Router or Broker, along
with the services common to those two roles, which basically is just
native and SQL queries. When the user first connects, `druid-client` does not know
if the URL provided is for a Router or Broker, so the client is a hybrid.

The `client`package provides classes to work with SQL queries along with a set
of handy functions and utility functions. These functions perform various
operations to simply SQL usage, work with Druid data types and time entities,
etc.

The `Client` class provides access to the cluster, cluster metadata, and
table metadata. To do this, we do just-in-time imports since we would otherwise
cause circular dependencies between the `client` and `cluster` modules.

The `client` package also defines the `Service` class: the base class for all
Druid "roles". The `Client` extends `Service` with the common Router/Broker
services as described above.

Each custer needs some amount of configuration, such as the port map for
Docker, or TLS certificates, etc. These reside in the various `config`
classes, and are passed along to each `Service`.

## Cluster

The `cluster` module extends the API to all Druid roles, and has a set of
service-specific classse. The `Router` and `Broker` classes extend the
`Client` class, adding their own specific services.

The `Cluster` class caches the `Service` subclass for each host. So, if the
application asks for the Coordinator three times, it will get the same
instane each time.

The `ClusterMetadata` builds on the roles to provide a more-or-less unified
view of cluster "metadata" (operational data.) Similarly, the
`TableMetadata` class builds a unified view about a single table (data source).
Both classes are just thin layers on either a SQL query to a system table,
or to a native API: they don't do any caching or consolidation of the
information.

## Extensions

Druid is well-known for its extreme extensibility. Users can add new native
query types, new services and more. `druid-client` attempts to echo this flexibility
by allowing applications to define custom roles:

* The role was given a name in Druid.
* Define the role as a subclass of `Service`.
* Call `cluster.register_service` to register the class with the name.

Now, you can request your service by name and `druid-client` will create instances
of your class and add them to its service cache.
