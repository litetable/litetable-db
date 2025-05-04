# LiteTable DB
**LiteTable is in active development and should not be considered stable.**

<img src="./images/litetable-logo-min.png" alt="LiteTable Logo" width="350px">

LiteTable is a light-weight (pun intended), high-performance key-value database designed for fast 
iteration and massive scale. Written in pure Go, it provides a simple, flexible and efficient 
storage solution inspired by column-oriented database systems like Google BigTable and Apache Cassandra.

## Key Features

- **Flexible Data Model**: Store data organized by row key, column family, and qualifiers
- **Versioned Values**: Support for time-versioned data entries
- **Durability**: Persistent storage with WAL for crash recovery
- **Performance**: In-memory operations with configurable flush thresholds
- **Security**: Optional TLS encryption for client-server communication
---
## Quick Start

LiteTable can be run locally or deployed via Docker:

### LiteTable CLI
To get started with LiteTable DB, it is recommended to use the LiteTable CLI. 

1. Install the CLI:
   ```bash
   go get github.com/litetable/litetable-cli
   ```
2. Initialize a new LiteTable database:
   ```bash
    litetable service init
    ```
   
3. Start the LiteTable server:
   ```bash
   litetable service start
   ```

4. Stop the LiteTable server:
   ```bash
   litetable service stop
   ```

With an initialized server, you can start writing data to it. The first write is to always 
create a supported column family, which is accomplished by a `create` command.
```bash
litetable create --family <my_family>
```

A valid column family is required for every read and write command.

### Create some data to your column family:
1. With a running server, create a new column family:
   ```bash
   litetable create --family wrestlers
   ```

2. Create a new record for that column family
   ```bash
      litetable write -k champ:1 -f wrestlers -q firstName -v John -q lastName -v Cena -q  championships -v 15
      ```
3. Append more data to the row key
   ```bash
      litetable write -k champ:1 -f champions -q championships -v 16 &&
      litetable write -k champ:1 -f champions -q championships -v 17
      ```
4. Read the data back
   ```bash
   litetable read -k champ:1 -f wrestlers
   ```

5. Delete a column qualifier 
   ```bash
   litetable delete -k champ:1 -f wrestlers -q championships
   ```
   
6. Delete with custom TTL (number of seconds before garbage collection)
   ```bash
   litetable delete -k champ:1 -f wrestlers -q championships --ttl 300
   ```
---
## Data Structure
Using the `write` command from above returns the following data.
```json
{
   "key": "user:012345",
   "cols": {
      "champions": {
         "championships": [
            {
               "value": "MTU=",
               "timestamp": "2025-04-26T23:53:38.998676-04:00"
            }
         ],
         "firstName": [
            {
               "value": "Sm9obg==",
               "timestamp": "2025-04-26T23:53:38.998676-04:00"
            }
         ],
         "lastName": [
            {
               "value": "Q2VuYQ==",
               "timestamp": "2025-04-26T23:53:38.998676-04:00"
            }
         ]
      }
   }
}
```

Notice the values are base64 encoded. This is done to ensure that the data is stored in a 
consistent format. All data can be decoded by the conventional ways in their respective languages.
---
## Querying Data
### Time-series entries
LiteTable queries can be configured to return all or the `latestNValue` of a column family. For 
example:
```bash
litetable read -k user:12345 -f champions -l 3
```

This will return the latest 3 entries for every column qualifier in the `champions` family.

```
rowKey: champ:1
family: wrestlers
  qualifier: name
    value 1: John (timestamp: 2025-04-27T00:08:38.15789-04:00)
  qualifier: lastName
    value 1: Cena (timestamp: 2025-04-27T00:08:38.15789-04:00)
  qualifier: championships
    value 1: 17 (timestamp: 2025-04-27T00:08:57.876812-04:00)
    value 2: 16 (timestamp: 2025-04-27T00:08:55.300799-04:00)
    value 3: 15 (timestamp: 2025-04-27T00:08:38.15789-04:00)
```
Because the `name` and `lastName` qualifiers only have less than 3 entries, the `-l` flag will 
return any rows < N.

---
To get the latest 3 entries for a specific column qualifier, you can use the `-q` flag:
```
litetable read -k champ:1 -f wrestlers -q championships -l 3
```

```
rowKey: champ:1
family: wrestlers
  qualifier: championships
    value 1: 17 (timestamp: 2025-04-27T00:08:57.876812-04:00)
    value 2: 16 (timestamp: 2025-04-27T00:08:55.300799-04:00)
    value 3: 15 (timestamp: 2025-04-27T00:08:38.15789-04:00)

```
## Wide Column, Time-Series Data Model
A wide column store allows every written record on a row prefix to have N number of columns 
where no two rows on the same rowKey are required to have the same columns.

### Proudly written in Go.
LiteTable DB is proudly written in Go and is designed with the modern developer in mind. 
Wide-column NoSQL is the same technology that powers applications like Google Maps, Google 
Sheets, Instagram and Netflix just to name a few.   
