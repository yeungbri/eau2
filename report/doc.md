# Introduction
eau2 is a distributed key-value store. It can store key-value pairs and retrieve keys from any node in the system. Each node can access other nodes very quickly to read from their respective key-value stores. Applications that run on top of each key-value store have access to all values in the system.

# Architecture
* A node is a client that registers with a master server, which enables it to communicate with other clients registered to that master server.
* Each node has its own key-value store (which may or may not be a part of a larger key-value store) and can run an application.
* Each key is unique and contains information about which node it's home to. 
* When retrieving a key in an application, if the key is not on the node's own key-value store, it will ask the key's home node for the value. 
* Values are dataframes which are fancy tables that can contain names for rows and columns and follow a particular schema defining the data types for each column.

# Implementation
## Server and Client (the network)
* The `Server` class manages nodes on the network. It does not store a key-value store of its own. It supports registration from incoming clients, facilitating communication between clients, and broadcasting its list of clients every time a client is added or removed.
* The `Client` class is a node on the network. It must register with the `Server` on startup. It is able to communicate to other clients upon registration. It is also able to independently shut down by removing itself from the server registry. The `Client` is responsible for responding to requests by a user to retrieve some value from a key-value store. 

## Dataframe (the "values" in our key-value store)
* The `Dataframe` class is a representation of a tabular data format, which contains a list of `Column` and a list of `Row`, both of which in turn may contain a name and various types of data. A user may gather data about the data in a `Dataframe` via various querying methods, which allow querying by row, column, or row-column coordinates. A user may also filter the `Dataframe` with specific conditions (such as by date or name) or map over the contents of a certain row.
* The `Schema` class is a meta-row in a `Dataframe` that defines the types of each of its columns. Be careful updating it, especially if it describes a very complex and large Dataframe. Errors may result in undefined behavior.

## Key-Value store (the lowest level)
* A `Key` is simply a C++ string that is formatted as `<node>_<name>`, where `<node>` is the name of the client that hosts this key, and `<name>` is the name of the key, which must be unique on its node.
* A `Value` is a `Dataframe` as described above.
* These pairs will be stored on disk on whatever machines are hosting each of the nodes. They can be accessed via queries by a user, which are executed by a `Client`.

# Use Cases


# Open Questions


# Status