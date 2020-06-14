import os
import collections as col

import sqlalchemy as sa


class ODBConnector:
    """
    Serves as an input/output connector for a local SQLite database.
    """
    @property
    def schemas(self):
        return self._schemas

    @property
    def tables(self):
        return self._tables

    def __init__(self):
        self.engine = None
        self._db_path = None
        self._tables = dict()
        self._schemas = dict()
        self._metadata = sa.MetaData()
        self._type_map = {str: sa.String, int: sa.Integer, float: sa.Float}

    def new_tbl(self, table: str, schema: dict) -> None:
        """
        Creates a new table in the connected db using the passed
        schema.

        Args:
            table: The name of the table to create.
            schema: A dictionary with column names as keys and
                python dtype objects found in self._type_map as values.

        Returns: None
        """
        columns = list()
        for name, dtype in schema.items():
            c = sa.Column(name, self._type_map[dtype])
            columns.append(c)
        self._tables[table] = sa.Table(table, self._metadata, *columns)
        self._tables[table].create(self.engine)
        self._schemas[table] = schema

    def drop_tbl(self, table: str) -> bool:
        """
        Drops the passed table from the connected db.

        Args:
            table: A string, the name of a table in the connected db
                and the ODBConnector's attributes.

        Returns: A boolean indicating whether the table was found and
            deleted.

        """
        if self._tables.get(table) is not None:
            t = self._tables[table]
            t.drop(self.engine)
            t.metadata.remove(t)
            self._tables.pop(table)
            self._schemas.pop(table)
            return True
        else:
            return False

    def insert(self, table: str, records, schema=None) -> None:
        """
        Takes the records in a list or DataFrame and inserts them into
        the connected db.

        Args:
            table: The name of the table to insert into. If this is
                a new table, then schema must be supplied.
            records: A list of dictionaries or DataFrame.
                Keys/columns must match the schema of the target table.
            schema: A dictionary with column names as keys and
                SQLAlchemy type objects as values. Optional if the
                target table already exists.

        Returns: None
        """
        if table not in self._tables.keys():
            if schema is None:
                raise ValueError(f'{table} does not exist. If inserting '
                                 f'into a new table, provide a schema.')
            else:
                self.new_tbl(table, schema)
        with self.engine.connect() as conn:
            inserts = []
            for r in records:
                values = {k: None for k in self._schemas[table].keys()}
                for k, v in r.items():
                    values[k] = v
                inserts.append(values)
            conn.execute(self._tables[table].insert(), inserts)

    def purge(self) -> bool:
        """
        A simple method that deletes the entire db file found at
        self._db_path. Useful when you need to rebuild a db from
        first principles.

        Returns: A boolean indicating whether a db was found and purged.
        """
        if os.path.exists(self._db_path):
            os.remove(self._db_path)
            print(f'Pre-existing db at {self._db_path} found and '
                  f'purged.')
            return True
        else:
            print(f'No db found at {self._db_path}, purge '
                  f'skipped.')
            return False

    def select(self, table: str) -> list:
        """
        Selects data from the connected db and stored in the passed
        table.

        Args:
            table: A valid table name from the db.

        Returns: A list of dictionaries, each value being a row from
            the passed table.
        """
        with self.engine.connect() as conn:
            results = []
            for r in conn.execute(sa.select([self._tables[table]])).fetchall():
                d = col.OrderedDict()
                for i, k in enumerate(self._schemas[table].keys()):
                    d[k] = r[i]
                results.append(d)
        return results

    def setup(self, db_path: str, purge=False) -> None:
        """
        Setups a connection to a sqlite database, either connecting to
        an existing db or creating a new one.

        Args:
            db_path: The path to the sqlite database.
            purge: A boolean indicating whether any existing dbs
                should be deleted before beginning setup.

        Returns: None
        """
        self._db_path = db_path
        if purge:
            self.purge()
        self.engine = sa.create_engine(f'sqlite:///' + self._db_path, echo=False)
        # Check if the database already exists, and load its schema
        # into the ODBConnector object:
        self._metadata.reflect(bind=self.engine)
        if len(self._metadata.tables) > 0:
            for n, t in self._metadata.tables.items():
                self._tables[n] = t
                self._schemas[n] = self._parse_sa_schema(t.c)

    @staticmethod
    def _parse_sa_schema(col_collect) -> dict:
        """
        When an existing db is found, this method is used to translate
        its tables' SQLAlchemy schemas back into simple dictionaries
        like those that would be passed to the new_tbl method.

        Args:
            col_collect: A SQLAlchemy ImmutableColumnCollection
                object.
            A dictionary with column names as keys and column
                types as values.

        Returns: A dictionary form of the table's schema.
        """
        s = dict()
        for k, v in col_collect.items():
            s[k] = v.type.python_type
        return s


def write_sqlite(odbc: ODBConnector, table_name: str, data: list,
                 schema: dict) -> None:
    """
    Simple function to write data to a sqlite db connected via an
    ODBConnector. Overwrites whatever data is in the existing table, if
    any.

    Args:
        odbc: An ODBConnector object.
        table_name: A string, the name of the table.
        data: A list of dicts to write to the table.
        schema: A dictionary containing the schema of the table.

    Returns:

    """
    odbc.drop_tbl(table_name)
    odbc.insert(table_name, data, schema)
