## pydl
A lightweight Delta Lake/Iceberg inspired database implementation in Python, featuring ACID transactions with snapshot isolation.

### Background

Delta Lake and Apache Iceberg are open-source storage frameworks that enable building scalable and reliable data lakes with ACID transactions. They provide features like schema enforcement and snapshot isolation, making them suitable for big data analytics.

Inspired by the simplicity of Delta Lake, this project implements similar concepts, focusing on core functionalities:

- **Immutable Data Files**: Data is stored in immutable files on disk.
- **Transaction Log**: Records metadata about data files, enabling snapshot isolation.
- **Atomic Operations**: Uses atomic file system operations to prevent conflicts and ensure only one transaction commits at a time.

### Implementation Overview

The project is built using the following key components:

- **Object Storage Interface**: Abstracts storage operations with methods for atomic writes, listing files by prefix, and reading files.
- **File-Based Storage**: Implements the object storage interface using the file system, leveraging atomic file operations for concurrency control.
- **Transactions**: Manages transaction states, including actions like adding data objects and changing metadata.
- **Data Objects**: Immutable files that store rows of data in JSON format.
- **Concurrency Control**: Uses atomic file system operations (`os.link`) to detect and prevent conflicting transactions.

### Example Usage

Here's how you can use this in your Python code:

```python
from delta_lake import FileObjectStorage, Client

# Initialize object storage
storage = FileObjectStorage('/path/to/data')

# Create a client
client = Client(storage)

# Start a new transaction
client.new_tx()

# Create a table
client.create_table('users', ['id', 'name'])

# Insert rows
client.write_row('users', [1, 'Alice'])
client.write_row('users', [2, 'Bob'])

# Commit the transaction
client.commit_tx()

# Start a read transaction
client.new_tx()

# Scan the table
iterator = client.scan('users')
for row in iterator:
    print(row)

# Commit the read transaction
client.commit_tx()
```

### Acknowledgments

I want to give full credit to **@eatonphil**. His implementation of **otf** in Go inspired me to read the Delta Lake paper and attempt this Python version. Here's the link to his [blog post](https://notes.eatonphil.com/2024-09-29-build-a-serverless-acid-database-with-this-one-neat-trick.html). And the [Delta Lake paper](https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf).
