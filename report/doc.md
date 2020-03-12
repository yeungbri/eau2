# Introduction
eau2 is a distributed key-value store. It can store key-value pairs and retrieve keys from any node in the system. Each node can access other nodes very quickly to read from their respective key-value stores. Applications that run on top of each key-value store have access to all values in the system.

# Architecture
* A node is a client that registers with a master server, which enables it to communicate with other clients registered to that master server.
* Each node has its own key-value store (which may or may not be a part of a larger key-value store) and can run an application.
* Each key is unique and contains information about which node it's home to. 
* When retrieving a key in an application, if the key is not on the node's own key-value store, it will ask the key's home node for the value. 
* Values are dataframes which are fancy tables that can contain names for rows and columns and follow a particular schema defining the data types for each column.

# Implementation
### Server and Client (the network)
* The `Server` class manages nodes on the network. It does not store a key-value store of its own. It supports registration from incoming clients, facilitating communication between clients, and broadcasting its list of clients every time a client is added or removed.
* The `Client` class is a node on the network. It must register with the `Server` on startup. It is able to communicate to other clients upon registration. It is also able to independently shut down by removing itself from the server registry. The `Client` is responsible for responding to requests by a user to retrieve some value from a key-value store. 

### Dataframe (the "values" in our key-value store)
* The `Dataframe` class is a representation of a tabular data format, which contains a list of `Column` and a list of `Row`, both of which in turn may contain a name and various types of data. A user may gather data about the data in a `Dataframe` via various querying methods, which allow querying by row, column, or row-column coordinates. A user may also filter the `Dataframe` with specific conditions (such as by date or name) or map over the contents of a certain row.
* The `Schema` class is a meta-row in a `Dataframe` that defines the types of each of its columns. Be careful updating it, especially if it describes a very complex and large Dataframe. Errors may result in undefined behavior.

### Key-Value store (the lowest level)
* A `Key` is simply a C++ string that is formatted as `<node>_<name>`, where `<node>` is the name of the client that hosts this key, and `<name>` is the name of the key, which must be unique on its node.
* A `Value` is a `Dataframe` as described above.
* These pairs will be stored on disk on whatever machines are hosting each of the nodes. They can be accessed via queries by a user, which are executed by a `Client`.

# Use Cases
To create a key-value pair and insert it into the global `kv` store:
```
Key k(4, "residents");   // key stored on node 4, with name residents
Schema s("SSI");           // Schema of string, string, int (name, address, age)
Dataframe residents(s);    // Dataframe that contains a table of residents
Row joe("Joe", "13 Main St", 41);
Row ann("Ann", "14 Main St", 43);
Row mary("Mary", "15 Main St", 6);
residents.addRow(joe);
residents.addRow(ann);
residents.addRow(mary);
kv.insert(k, residents);
```
To access some value and return it:
```
Key k(4, "residents");
// Retrieves the value from the store it's on, even if it is NOT on this node!
auto val = kv.get(k);
if (val)
{
    return val;
} else
{
    // The key does not exist on node 4, proceed accordingly
    throw std::runtime_exception("Key does not exist at node " + std::to_string(k.node());
}
```
More use cases will be added as this system is developed.

# Open Questions
* How will the key-value store be stored on each node?
* What is the most efficient way to transfer values (Dataframes) over a network?
* What do we do if the value exists, but the key specifies the wrong node?
* How reliable is the network? How much bandwidth does it support (can we send large values over the network)?
* Which layer is responsible for network communication (should the kv store or the application do the querying)?
* What are the performance benchmarks?

# Status
### Done so far:
* The Dataframe can be read in and internally constructed from any Schema-on-read (sor) file. 
* The network is fairly robust and can handle new client registration and teardown, as well as support direct messages between clients. The server is able to shut down and initiate a clean network teardown as well. 
* The serialization code to be used to send values over the network is still in progress but looking in good shape.

### TODO:
* Implement a robust key-value store for storing many, many key-value pairs. This will definitely be the most difficult part.
* Implement a key implementation that can be used to easily identify and access the node that the value is on.
* Create lots of tests for our existing code, and our new code!

### Time estimates:
* Key-value store: 20 hours
* Key implementation: 5 hours
* Tests: 10 hours
* General debugging: 20 hours
* TOTAL: 55 hours