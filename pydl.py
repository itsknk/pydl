import os
import sys
import threading
import uuid
import json
import shutil
import tempfile

# Utility Functions

DEBUG = '--debug' in sys.argv

def assert_(condition, msg):
    if not condition:
        raise AssertionError(msg)

def assert_eq(a, b, prefix=''):
    if a != b:
        raise AssertionError(f"{prefix} '{a}' != '{b}'")

def debug(*args):
    if DEBUG:
        print('[DEBUG]', *args)

def uuidv4():
    return str(uuid.uuid4())

# Object Storage Interface

class ObjectStorage:
    def put_if_absent(self, name, bytes_):
        raise NotImplementedError

    def list_prefix(self, prefix):
        raise NotImplementedError

    def read(self, name):
        raise NotImplementedError

# File-Based Object Storage Implementation

class FileObjectStorage(ObjectStorage):
    def __init__(self, basedir):
        self.basedir = basedir
        os.makedirs(self.basedir, exist_ok=True)
        self.lock = threading.Lock()

    def put_if_absent(self, name, bytes_):
        tmpfilename = os.path.join(self.basedir, uuidv4())
        with open(tmpfilename, 'wb') as f:
            f.write(bytes_)

        filename = os.path.join(self.basedir, name)

        try:
            # Use atomic os.link if possible?!
            os.link(tmpfilename, filename)
            os.remove(tmpfilename)
        except FileExistsError:
            os.remove(tmpfilename)
            raise FileExistsError(f"File {filename} already exists")
        except Exception as e:
            os.remove(tmpfilename)
            raise e

    def list_prefix(self, prefix):
        files = os.listdir(self.basedir)
        return [f for f in files if f.startswith(prefix)]

    def read(self, name):
        filename = os.path.join(self.basedir, name)
        with open(filename, 'rb') as f:
            return f.read()

# Action Types and Serialization

class DataobjectAction:
    def __init__(self, name, table):
        self.name = name
        self.table = table

    def to_dict(self):
        return {'AddDataobject': {'Name': self.name, 'Table': self.table}}

class ChangeMetadataAction:
    def __init__(self, table, columns):
        self.table = table
        self.columns = columns

    def to_dict(self):
        return {'ChangeMetadata': {'Table': self.table, 'Columns': self.columns}}

class Action:
    def __init__(self, add_dataobject=None, change_metadata=None):
        self.add_dataobject = add_dataobject
        self.change_metadata = change_metadata

    def to_dict(self):
        if self.add_dataobject is not None:
            return self.add_dataobject.to_dict()
        elif self.change_metadata is not None:
            return self.change_metadata.to_dict()
        else:
            return {}

# DataObject Class

DATAOBJECT_SIZE = 64 * 1024

class DataObject:
    def __init__(self, table, name, data, length):
        self.table = table
        self.name = name
        self.data = data
        self.length = length

    def to_dict(self):
        return {
            'Table': self.table,
            'Name': self.name,
            'Data': self.data,
            'Len': self.length
        }

# Transaction Class

class Transaction:
    def __init__(self, id_):
        self.id = id_
        self.previous_actions = {}  # {table_name: [Action]}
        self.actions = {}           # {table_name: [Action]}
        self.tables = {}            # {table_name: [columns]}
        self.unflushed_data = {}    # {table_name: [rows]}

# Custom Exceptions

class ExistingTransactionError(Exception):
    pass

class NoTransactionError(Exception):
    pass

class TableExistsError(Exception):
    pass

class NoSuchTableError(Exception):
    pass

class ConcurrentCommitError(Exception):
    pass

# Client Class

class Client:
    def __init__(self, os_):
        self.os = os_
        self.tx = None

    def new_tx(self):
        if self.tx is not None:
            raise ExistingTransactionError("Existing Transaction")

        log_prefix = "_log_"
        tx_log_filenames = self.os.list_prefix(log_prefix)
        tx_log_filenames.sort()  # Ensure transactions are processed in order

        max_id = 0
        tx = Transaction(0)

        for tx_log_filename in tx_log_filenames:
            bytes_ = self.os.read(tx_log_filename)
            old_tx_data = json.loads(bytes_)
            old_tx = Transaction(old_tx_data['Id'])
            max_id = max(max_id, old_tx.id)

            # Reconstruct previous actions
            for table, actions in old_tx_data['Actions'].items():
                for action_data in actions:
                    if 'AddDataobject' in action_data:
                        ada = action_data['AddDataobject']
                        action = Action(add_dataobject=DataobjectAction(ada['Name'], ada['Table']))
                        tx.previous_actions.setdefault(table, []).append(action)
                    elif 'ChangeMetadata' in action_data:
                        cmd = action_data['ChangeMetadata']
                        action = Action(change_metadata=ChangeMetadataAction(cmd['Table'], cmd['Columns']))
                        tx.tables[table] = cmd['Columns']
                        tx.previous_actions.setdefault(table, []).append(action)
                    else:
                        raise Exception(f"Unsupported action: {action_data}")

        tx.id = max_id + 1
        self.tx = tx

    def create_table(self, table, columns):
        if self.tx is None:
            raise NoTransactionError("No Transaction")

        if table in self.tx.tables:
            raise TableExistsError("Table Exists")

        self.tx.tables[table] = columns

        action = Action(change_metadata=ChangeMetadataAction(table, columns))
        self.tx.actions.setdefault(table, []).append(action)

    def write_row(self, table, row):
        if self.tx is None:
            raise NoTransactionError("No Transaction")

        if table not in self.tx.tables:
            raise NoSuchTableError("No Such Table")

        data = self.tx.unflushed_data.setdefault(table, [])
        data.append(row)

        if len(data) >= DATAOBJECT_SIZE:
            self.flush_rows(table)

    def flush_rows(self, table):
        if self.tx is None:
            raise NoTransactionError("No Transaction")

        data = self.tx.unflushed_data.get(table)
        if not data:
            return

        df = DataObject(table, uuidv4(), data.copy(), len(data))
        bytes_ = json.dumps(df.to_dict()).encode('utf-8')

        filename = f"_table_{table}_{df.name}"
        self.os.put_if_absent(filename, bytes_)

        action = Action(add_dataobject=DataobjectAction(df.name, table))
        self.tx.actions.setdefault(table, []).append(action)

        # Reset in-memory data
        self.tx.unflushed_data[table] = []

    def scan(self, table):
        if self.tx is None:
            raise NoTransactionError("No Transaction")

        dataobjects = []
        all_actions = self.tx.previous_actions.get(table, []) + self.tx.actions.get(table, [])

        for action in all_actions:
            if action.add_dataobject is not None:
                dataobjects.append(action.add_dataobject.name)

        unflushed_rows = self.tx.unflushed_data.get(table, [])

        return ScanIterator(self, table, unflushed_rows, dataobjects)

    def commit_tx(self):
        if self.tx is None:
            raise NoTransactionError("No Transaction")

        # Flush any outstanding data
        for table in self.tx.tables:
            self.flush_rows(table)

        wrote = any(self.tx.actions.values())

        if not wrote:
            self.tx = None
            return

        filename = f"_log_{self.tx.id:020d}"
        tx_data = {
            'Id': self.tx.id,
            'Actions': {table: [action.to_dict() for action in actions] for table, actions in self.tx.actions.items()}
        }
        bytes_ = json.dumps(tx_data).encode('utf-8')

        try:
            self.os.put_if_absent(filename, bytes_)
            self.tx = None
        except FileExistsError:
            self.tx = None
            raise ConcurrentCommitError("Concurrent Commit Failed")

# Scan Iterator

class ScanIterator:
    def __init__(self, client, table, unflushed_rows, dataobjects):
        self.client = client
        self.table = table
        self.unflushed_rows = unflushed_rows
        self.unflushed_row_pointer = 0
        self.dataobjects = dataobjects
        self.dataobjects_pointer = 0
        self.dataobject = None
        self.dataobject_row_pointer = 0

    def __iter__(self):
        return self

    def __next__(self):
        # Iterate over unflushed rows
        if self.unflushed_row_pointer < len(self.unflushed_rows):
            row = self.unflushed_rows[self.unflushed_row_pointer]
            self.unflushed_row_pointer += 1
            return row

        # Move to the next data object if necessary
        while self.dataobject is None or self.dataobject_row_pointer >= self.dataobject.length:
            if self.dataobjects_pointer >= len(self.dataobjects):
                raise StopIteration

            name = self.dataobjects[self.dataobjects_pointer]
            bytes_ = self.client.os.read(f"_table_{self.table}_{name}")
            dataobject_data = json.loads(bytes_)
            self.dataobject = DataObject(
                dataobject_data['Table'],
                dataobject_data['Name'],
                dataobject_data['Data'],
                dataobject_data['Len']
            )
            self.dataobjects_pointer += 1
            self.dataobject_row_pointer = 0

        row = self.dataobject.data[self.dataobject_row_pointer]
        self.dataobject_row_pointer += 1
        return row

# Main Function (Optional)

def main():
    pass  # Unimplemented

# Testing Code

def test_concurrent_table_writers():
    dirpath = tempfile.mkdtemp()

    try:
        fos = FileObjectStorage(dirpath)
        c1_writer = Client(fos)
        c2_writer = Client(fos)

        # c2_writer starts a transaction
        c2_writer.new_tx()
        debug("[c2] new tx")

        # c1_writer starts and commits a transaction first
        c1_writer.new_tx()
        debug("[c1] new tx")
        c1_writer.create_table("x", ["a", "b"])
        debug("[c1] Created table")
        c1_writer.write_row("x", ["Joey", 1])
        debug("[c1] Wrote row")
        c1_writer.write_row("x", ["Yue", 2])
        debug("[c1] Wrote row")
        c1_writer.commit_tx()
        debug("[c1] Committed tx")

        # c2_writer attempts to create the same table and write data
        try:
            c2_writer.create_table("x", ["a", "b"])
            debug("[c2] Created table")
            c2_writer.write_row("x", ["Holly", 1])
            debug("[c2] Wrote row")
            c2_writer.commit_tx()
            debug("[c2] Committed tx")
        except Exception as e:
            debug("[c2] tx not committed due to error:", e)
            # Expected error due to conflict

    finally:
        shutil.rmtree(dirpath)

def test_concurrent_reader_with_writer_reads_snapshot():
    dirpath = tempfile.mkdtemp()

    try:
        fos = FileObjectStorage(dirpath)
        c1_writer = Client(fos)
        c2_reader = Client(fos)

        # First create some data and commit the transaction.
        c1_writer.new_tx()
        debug("[c1Writer] Started tx")
        c1_writer.create_table("x", ["a", "b"])
        debug("[c1Writer] Created table")
        c1_writer.write_row("x", ["Joey", 1])
        debug("[c1Writer] Wrote row")
        c1_writer.write_row("x", ["Yue", 2])
        debug("[c1Writer] Wrote row")
        c1_writer.commit_tx()
        debug("[c1Writer] Committed tx")

        # Now start a new transaction for more edits.
        c1_writer.new_tx()
        debug("[c1Writer] Starting new write tx")

        # Before we commit this second write-transaction, start a read transaction.
        c2_reader.new_tx()
        debug("[c2Reader] Started tx")

        # Write and commit rows in c1.
        c1_writer.write_row("x", ["Ada", 3])
        debug("[c1Writer] Wrote third row")

        # Scan x in read-only transaction
        iterator = c2_reader.scan("x")
        debug("[c2Reader] Started scanning")
        seen = 0
        for row in iterator:
            debug("[c2Reader] Got row in reader tx", row)
            if seen == 0:
                assert_eq(row[0], "Joey", "row mismatch in c2Reader")
                assert_eq(row[1], 1, "row mismatch in c2Reader")
            elif seen == 1:
                assert_eq(row[0], "Yue", "row mismatch in c2Reader")
                assert_eq(row[1], 2, "row mismatch in c2Reader")
            seen += 1
        assert_eq(seen, 2, "expected two rows")
        debug("[c2Reader] Done scanning")

        # Scan x in c1 write transaction
        iterator = c1_writer.scan("x")
        debug("[c1Writer] Started scanning")
        seen = 0
        for row in iterator:
            debug("[c1Writer] Got row in tx", row)
            if seen == 0:
                assert_eq(row[0], "Ada", "row mismatch in c1Writer")
                assert_eq(row[1], 3, "row mismatch in c1Writer")
            elif seen == 1:
                assert_eq(row[0], "Joey", "row mismatch in c1Writer")
                assert_eq(row[1], 1, "row mismatch in c1Writer")
            elif seen == 2:
                assert_eq(row[0], "Yue", "row mismatch in c1Writer")
                assert_eq(row[1], 2, "row mismatch in c1Writer")
            seen += 1
        assert_eq(seen, 3, "expected three rows")
        debug("[c1Writer] Done scanning")

        # Writer committing should succeed.
        c1_writer.commit_tx()
        debug("[c1Writer] Committed tx")

        # Reader committing should succeed.
        c2_reader.commit_tx()
        debug("[c2Reader] Committed tx")

    finally:
        shutil.rmtree(dirpath)

if __name__ == "__main__":
    # Run tests
    test_concurrent_table_writers()
    test_concurrent_reader_with_writer_reads_snapshot()

