import os
import collections as col

import sqlalchemy as sa
import pandas as pd

import datagenius.util as u


class ODBConnector:
    """
    Serves as an input/output connector for a local SQLite database.
    """
    type_map = {
        'O': str,
        'object': str,
        'string': str,
        'float64': float,
        'int64': int,
    }

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

    def insert(
            self,
            table: str,
            df: pd.DataFrame,
            schema=None) -> None:
        """
        Takes the records in a DataFrame and inserts them into
        the connected db.

        Args:
            table: The name of the table to insert into. If this is
                a new table, then schema must be supplied.
            df: A pandas DataFrame. Columns must match the schema of
                the target table. If no schema is passed one will
                be created based on the DataFrame's columns and dtypes.
            schema: A dictionary with column names as keys and
                SQLAlchemy type objects as values. Optional if the
                target table already exists.

        Returns: None
        """
        if table not in self._tables.keys():
            schema = gen_schema(df) if schema is None else schema
            self.new_tbl(table, schema)
        df = df.applymap(self._prep_object_dtype)
        with self.engine.connect() as conn:
            conn.execute(
                self._tables[table].insert(),
                df.to_dict('records', into=col.OrderedDict)
            )

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
            for r in conn.execute(sa.select(
                    [self._tables[table]])).fetchall():
                d = col.OrderedDict()
                for i, k in enumerate(self._schemas[table].keys()):
                    d[k] = u.translate_null(r[i])
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
        self.engine = sa.create_engine(
            f'sqlite:///' + self._db_path, echo=False)
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

    @staticmethod
    def _prep_object_dtype(x):
        """
        Pandas O/object dtype is a catch all and thus could contain
        python objects or other values that SQLalchemy and sqlite won't
        accept. Thus, anything not in an acceptable data type must be
        converted to its string representation.

        Args:
            x: Any value.

        Returns: The value, or a string representation of the value if
            the object's type is one of those listed below.

        """
        if type(x) not in (str, float, int):
            return str(x)
        else:
            return x


def from_sqlite(dir_path: str, table: str, **options) -> pd.DataFrame:
    """
    Creates a pandas DataFrame from a sqlite database table.

    Args:
        dir_path: The directory path where the db file is located.
        table: A string, the name of the table to pull data from.
        **options: Key-value options to alter to_sqlite's behavior.
            Currently in use options:
                db_conn: An io.odbc.ODBConnector object if you have
                    one, otherwise from_sqlite will create it.
                db_name: A string, the name of the db file to pull
                    from. Default is 'datasets'.

    Returns: A pandas DataFrame containing the contents of the
        passed table.

    """
    conn = quick_conn_setup(
        dir_path,
        options.get('db_name'),
        options.get('db_conn')
    )
    return pd.DataFrame(conn.select(table))


def convert_pandas_type(pd_dtype) -> object:
    """
    Convenience function for quickly converting pandas dtype strings
    to equivalent python objects.

    Args:
        pd_dtype: A pandas dtype object.

    Returns: The corresponding python object.

    """
    if str(pd_dtype) not in ODBConnector.type_map.keys():
        pd_dtype = 'O'
    return ODBConnector.type_map[str(pd_dtype)]


def gen_schema(df: pd.DataFrame) -> dict:
    """
    Generates a schema dictionary usable by ODBConnector off a pandas
    DataFrame.

    Args:
        df: A DataFrame

    Returns: A schema dictionary based off the columns and dtypes of
        the passed DataFrame.

    """
    return {
        k: convert_pandas_type(v) for k, v in df.dtypes.to_dict().items()
    }


def quick_conn_setup(dir_path, db_name=None, db_conn=None):
    """
    Convenience method for creating a sqlite database or connecting
    to an existing one.

    Args:
        dir_path: The directory path where the db file is/should
            be located.
        db_name: An io.odbc.ODBConnector object if you have
            one, otherwise to_sqlite will create it.
        db_conn:

    Returns:

    """
    db_name = 'datasets' if not db_name else db_name
    db_conn = ODBConnector() if not db_conn else db_conn
    db_conn.setup(os.path.join(dir_path, db_name + '.db'))
    return db_conn


def write_sqlite(
        odbc: ODBConnector,
        table_name: str,
        df: pd.DataFrame,
        schema: dict = None) -> None:
    """
    Simple function to write data to a sqlite db connected via an
    ODBConnector. Overwrites whatever data is in the existing table, if
    any.

    Args:
        odbc: An ODBConnector object.
        table_name: A string, the name of the table.
        df: A pandas DataFrame
        schema: A dictionary containing the schema of the table.

    Returns:

    """
    odbc.drop_tbl(table_name)
    schema = gen_schema(df) if schema is None else schema
    odbc.insert(table_name, df, schema)
