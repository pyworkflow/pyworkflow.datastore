# pyworkflow.datastore

## datastore backend for pyworkflow

[pyworkflow](http://github.com/pyworkflow/pyworkflow) supports the easy implementation of workflows, and handling the
execution of workflow processes, across multiple backends. This is the
backend for pyworkflow that stores execution state to a [datastore](https://github.com/datastore/datastore).

## Usage

DatastoreBackend provides a simple backend that stores execution state to a
datastore [https://github.com/datastore/datastore](https://github.com/datastore/datastore). It is mainly useful 
during development.

````python
from datastore.filesystem import FileSystemDatastore()
from pyworkflow.datastore import DatastoreBackend
from pyworkflow.managed import Manager

ds = FileSystemDatastore('/tmp/.pyworkflow_datastore')
backend = DatastoreBackend(ds)
manager = Manager(backend=backend)
````

## About

### License

pyworkflow.datastore is under the MIT License.

### Contact

pyworkflow.datastore is written by [Willem Bult](https://github.com/willembult).

Project Homepage: [https://github.com/pyworkflow/pyworkflow.datastore](https://github.com/pyworkflow/pyworkflow.datastore)

Feel free to contact me. But please file issues in github first. Thanks!
