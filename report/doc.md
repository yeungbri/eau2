# Introduction
eau2 is a distributed key value store. It can store key value pairs and retrieve keys from any node in the system. Applications that run on top of each key value store have access to all values in the system.

# Architecture
Each node has its own key-value store and can run an application. Each key is home to one key-value store and contains information about which node it's home to. When retrieving a key in an application, if the key is not on the node's own key-value store, it will ask the key's home node for the value. Values are dataframes which are fancy tables that can contain names for rows and columns and follow a particular schema defining the data types for each column.

# Implementation


# Use Cases


# Open Questions

# Status