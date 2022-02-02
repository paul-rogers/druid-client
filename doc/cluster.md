# Druid Cluster API

Druid clusters contain a variety of *services* (also called *roles* or *server types*.) Druid services reside in a *server*: a Java process. In most cases, there is one service per server, though it is possible to run both the Coordinator and Overlord in a single server.

In a smaller cluster, several servers may run on one machine, each on a distinct port. In a larger cluster, each machine may run a single server. In the largest clusters, multiple copies of each server may run on different machines. Druid identifies each service (or service replica) via a *service url*, which is a triple of (prototol, host name, port). When the Coordinator and Overlord run in the same server, they share the same port. In this case, the unique identifier is a quad of (service, prototol, host name, port).

Druid provides a system table, queryable using SQL, that lists the services in the cluster and their endpoints. `druid-client` uses the `SERVERS` system table to infer the services and servers in your cluster.

```python
import druid_client as dcl

client = dcl.connect('http://localhost:8888')
cluster = client.cluster()
cluster.services()
```

The output of the above is the list of services within the cluster:

```text
[{'server': '172.18.0.4:8081',
  'host': '172.18.0.4',
  'plaintext_port': 8081,
  'tls_port': -1,
  'server_type': 'coordinator',
  'tier': None,
  'curr_size': 0,
  'max_size': 0,
  'is_leader': 1},
  ...
```

## Service Client

Each service within the cluster has a specialized `druid-client` client. For example, to get the Coordinator client:

```python
coord = cluster.coord()
```

The clients are:

* `coord()` - Provides the lead Coordinator
* `overlord()`- Provides the lead Overlord.
* `broker()` - Provides the Broker. If more than one, picks the first.
* `router()` - Provides the Router. If more than one, picks the first.

The `client` above is a generic query client. Upon startup, `druid-client` does not know if the URL you provided is for a Router or a Broker, so the `Client` provided is the common subset. You can obtain your client from the cluster:

```python
client = cluster.client()
```

## API Support

`druid-client` provides support for much of the Druid REST API. Each client provides detailed help:

```python
help(client)
```

`druid-client` does not support all the Druid APIs. In some cases, the Druid team encourages the use of the SQL system tables instead of REST APis that provide the same information. In other cases, the `druid-client` team has not yet implemented the API. (Contributions welcome!)

Most APIs are just a simple Python method. For example, for the APIs common to all services:

```python
coord.is_healthy()

>>> True
```

## Configuration

In simple cases, the `connect()` call shown above is all you need. However, there are cases where you must provide additional configuration:

* Your cluster runs in Docker, Kubernetes or AWS so that the host IP or port visible to `druid-client` is different than that known within the cluster itself.
* Your cluster provides both HTTP (AKA "plain text") and HTTPS (AKA "TLS"), ports and you prefer to use TLS.
* Your cluster requires a private TLS certificate.

In all these cases, you must specify additional configuration in the connect call.

To prefer TLS:

```python
url = 'http://localhost:8888'
client = dcl.connect(url, prefer_tls=True)
```

To provide a private TLS certificate:

```python
client = dcl.connect(url, tls_cert=file_name)
```

## URL Map

The out-of-the-box Docker Compose script creates a cluster that works fine with `druid-client` with no additional configuration. However, if you change the port mapping, or Docker runs on another machine, then you use the `DockerMapper` to provide the needed translation. Example if you change a port mapping:

```python
from druid-client.client.config import DockerMapper

docker_port_map = [[8899, 8888]]
url_mapper = DockerMapper(port_map=docker_port_map)
client = dcl.connect("http://localhost:8899", mapper=url_mapper)
```

The port map lists the port mappings in the same order as Docker: `[external_port, internal_port]`.

If you need a different translation, simply write the required mapper. See the source code for `DockerMapper` to see what is required.

## OLD OLD OLD


`druid-client` represents the Druid servies via a `Service` class, with subclasses for each service. `druid-client` uses the servies system table to learn the services available within your cluster. Then, to use a service, you ask for a `Service` object for that service, then use methods that access each of that services's APIs.

The process starts by connecting to your Router or Broker as shown earlier, then request the cluster:

```python
import druid_client as dcl

client = dcl.connect('http://localhost:8888')
cluster = client.cluster()
cluster.services()
```

Next, request the services. The `client` itself is already the `Router` service:

```python
typeof(client)
>> Router
```

The most commonly used services are the (lead) Coordinator and (lead) Overlord:

```python
coord = cluster.coord()
overlord = cluster.overlord()
```

## Generic Service Access

Every endpoint is associated with a service. To get the service for an endpoint:

```python
endpoint = 'http://localhost:8888'
cluster.what_is(endpoint)
>> Router
service = cluster.service(endpoint)
typeof(service)
>> Router
```

### REST API

`druid-client` provides wrappers for  (but not all) REST API messages. You can create custom messages by working directly with the client layer:

```python
# Example needed
```

This form is handy if you run a task in the Indexer which exposes custom REST APIs.
