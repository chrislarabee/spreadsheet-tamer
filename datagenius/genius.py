import os
from typing import Callable

import pandas as pd
import numpy as np

import datagenius.lib as lib
import datagenius.util as u
import datagenius.metadata as md
from datagenius.io import odbc, text
from datagenius.tms_registry import TMS


@pd.api.extensions.register_dataframe_accessor("genius")
class GeniusAccessor:
    """
    A custom pandas DataFrame accessor that adds a number of additional
    methods and properties that extend the DataFrame's functionality.
    """

    def __init__(self, df: pd.DataFrame):
        """

        Args:
            df: A pandas DataFrame.
        """
        self.df = df

    def preprocess(
        self,
        header_func: Callable = lib.preprocess.detect_header,
        metadata: md.GeniusMetadata = None,
        **options,
    ) -> tuple:
        """
        A convenient way to run functions from lib.preprocess on
        self.df.

        Args:
            header_func: A callable object that takes a Dataset object
                and kwargs.
            metadata: A GeniusMetadata object.
            **options: Keyword args. See the preprocess transmutations
                for details on the arguments they take.
        Returns: self.df, modified by preprocess transmutations, and a
            metadata dictionary describing the changes made.

        """
        pp_tms = [*TMS["preprocess"]]
        if u.gwithin(self.df.columns, r"[Uu]nnamed:*[ _]\d") or isinstance(
            self.df.columns, pd.RangeIndex
        ):
            pp_tms.insert(0, lib.preprocess.purge_pre_header)
            pp_tms.insert(0, header_func)
        return self.transmute(*pp_tms, metadata=metadata, **options)

    def explore(self, metadata: md.GeniusMetadata = None) -> tuple:
        """
        A convenient way to run functions from lib.explore on self.df.

        Args:
            metadata: A GeniusMetadata object.

        Returns: self.df, and a metadata dictionary describing explore
            results.

        """
        return self.transmute(*TMS["explore"], metadata=metadata)

    def clean(self, metadata: md.GeniusMetadata = None, **options) -> tuple:
        """
        A convenient way to run functions from lib.clean on self.df.

        Args:
            metadata: A GeniusMetadata object.
            **options: Keyword args. See the clean transmutations
                for details on the arguments they take.

        Returns: self.df, modified by clean transmutations, and a
            metadata dictionary describing the changes made.

        """
        cl_tms = self._align_tms_with_options(TMS["clean"], options)
        return self.transmute(*cl_tms, metadata=metadata, **options)

    def reformat(self, metadata: md.GeniusMetadata = None, **options) -> tuple:
        """
        A convenient way to run functions from lib.reformat on
        self.df.

        Args:
            metadata: A GeniusMetadata object.
            **options: Keyword args. See the reformat transmutations
                for details on the arguments they take.

        Returns: self.df, modified by reformat transmutations, and a
            metadata dictionary describing the changes made.

        """
        rf_tms = self._align_tms_with_options(TMS["reformat"], options)
        return self.transmute(*rf_tms, metadata=metadata, **options)

    def standardize(self, metadata: md.GeniusMetadata = None, **options) -> tuple:
        """
        A convenient way to run standardize functions from lib.clean on
        self.df.

        Args:
            metadata: A GeniusMetadata object.
            **options: Keyword args. See the clean transmutations
                for details on the arguments they take.

        Returns: self.df, modified by standardize transmutations, and a
            metadata dictionary describing the changes made.

        """
        st_tms = self._align_tms_with_options(TMS["standardize"], options)
        return self.transmute(*st_tms, metadata=metadata, **options)

    @staticmethod
    def _align_tms_with_options(tms: list, options: dict) -> list:
        """
        Takes a list of transmutations and returns only those with all
        their required args in options.

        Args:
            tms: A list of transmutations.
            options: A dictionary of options kwargs.

        Returns: A list of the transmutations in tms that have
            sufficient corresponding kwargs in options.

        """
        result = []
        for tm in tms:
            if None not in u.align_args(tm, options, "df").values():
                result.append(tm)
        return result

    def transmute(
        self, *transmutations, metadata: md.GeniusMetadata = None, **options
    ) -> tuple:
        """
        Executes the passed transmutation list on self.df with the
        passed options and using the passed metadata.

        Args:
            transmutations: A list of the transmutations in the stage.
            metadata: A GeniusMetadata object. If None is passed, one
                will be created.
            **options: Keyword args that will be passed to each
                relevant transmutation in transmutations.

        Returns: A tuple of self.df and the metadata object.

        """
        metadata = md.GeniusMetadata() if metadata is None else metadata
        transmutations = self._order_transmutations(transmutations)
        self.df = metadata(self.df, *transmutations, **options)
        return self.df, metadata

    def supplement(
        self,
        *other,
        on: (str, list, tuple),
        select_cols: (str, tuple) = None,
        suffixes: (str, tuple) = None,
        split_results: bool = False,
    ) -> (pd.DataFrame, tuple):
        """

        Args:
            *other: An arbitrary list of DataFrames to supplement
                self.df with.
            on: A str or list of column names, tuples of column
                names and dictionary conditions, or SupplementGuide
                objects. All columns referenced must be in self.df
                and the passed other DataFrames.
            select_cols: A list of column names in the other
                DataFrames that you want to include in the results.
                Useful if you only want some of the columns in the
                other DataFrames.
            suffixes: A string or tuple of strings, the suffixes you
                would like to append to columns in the other frames
                that have overlapping column names in the other frames.
                If passed, you must pass as many suffixes as the length
                of other.
            split_results: A boolean, set to True if you want to return
                two DataFrames, one which contains the rows from the
                primary DataFrame and the successfully matched rows
                from the other DataFrames. Otherwise, will return a
                single DataFrame containing all the rows in the primary
                frame with the successfully matched rows joined onto it.

        Returns: A single supplemented DataFrame or tuple of a
            supplemented DataFrame and unmatched rows from the primary
            DataFrame, depending on whether split_results is True.

        """
        frames = [self.df, *other]
        ons = lib.supplement.prep_ons(on)
        chunks, remainder = lib.supplement.chunk_dframes(
            lib.supplement.build_plan(ons), *frames
        )
        suffixes = lib.supplement.prep_suffixes(suffixes, len(other))
        select_cols = u.tuplify(select_cols)
        results = []
        p_cols = set(frames[0].columns)
        for sg in chunks:
            p_frame = sg.chunks[0]
            o_frames = sg.chunks[1:]
            for i, other in enumerate(o_frames):
                rsuffix = suffixes[i]
                if not other.empty:
                    o_cols = set(other.columns)
                    other["merged_on"] = ",".join(sg.on)
                    other = (
                        other[
                            {
                                *sg.on,
                                *o_cols.intersection(set(select_cols)),
                                "merged_on",
                            }
                        ]
                        if select_cols
                        else other
                    )
                    if sg.inexact:
                        p_frame = lib.supplement.do_inexact(
                            p_frame, other, sg.on, sg.thresholds, sg.block, rsuffix
                        )
                    else:
                        p_frame = lib.supplement.do_exact(
                            p_frame, other, sg.on, rsuffix
                        )
            results.append(p_frame)
        result_df = pd.concat(results)
        unmatched = result_df[result_df["merged_on"].isna()]
        matched = result_df[~result_df["merged_on"].isna()]
        unmatched = pd.concat([unmatched[p_cols], remainder])
        if split_results:
            return matched, unmatched
        else:
            return pd.concat([matched, unmatched])

    def apply_strf(
        self, *columns, strf: Callable = None, **col_strf_map
    ) -> pd.DataFrame:
        """
        Applies passed string function(s) to indicated columns. Skips
        nan values and non-string values.

        Just pass strf if you want to apply it to every value.

        Args:
            *columns: An arbitrary list of column names, all of which
                will have strf applied to them if they contain strings.
            strf: A Callable function that takes a string and returns
                a formatted string. If you want to use upper/lower/etc
                pass str.upper/str.lower/etc.
            **col_strf_map: Kwargs version of columns and strf, each
                key should be a column and each

        Returns: The passed DataFrame with the indicated string columns
            reformatted with the passed string function.

        """
        if columns is None and col_strf_map is None:
            col_strf_map = {c: strf for c in self.df.columns}
        col_strf_map = {**col_strf_map, **{c: strf for c in columns}}
        for c, f in col_strf_map.items():
            self.df[c] = self.df[c].apply(lambda x: f(x) if isinstance(x, str) else x)
        return self.df

    def multiapply(self, func: Callable, *columns, **kwargs) -> pd.DataFrame:
        """
        Convenience method for running the same function on one or more
        columns of the DataFrame with the same arguments. Avoids having
        to write out df['colx'] = df['colx'].apply(...) repeatedly.

        Args:
            func: A function that takes at least one argument.
            *columns: Column labels from self.df.
            **kwargs: Keyword args expected by func.

        Returns: The DataFrame, with the specified columns modified by
            the specified column.

        """
        kwargs = u.align_args(func, kwargs)
        for c in columns:
            if len(kwargs) > 1:
                self.df[c] = self.df[c].apply(func, **kwargs)
            else:
                self.df[c] = self.df[c].apply(func)
        return self.df

    def fillna_shift(self, *columns) -> pd.DataFrame:
        """
        Takes at least two columns in the DataFrame shifts all their
        values "leftward", replacing nan values. Basically, a given
        column's value will be moved "left" in the reverse of the order
        specified by columns until it hits the end or a non-null value.

        So:
            a   b   c            a   b   c
        0   1   nan 2        0   1   2   nan
        1   nan 3   4   ->   1   3   4   nan
        2   nan nan 5        2   5   nan nan

        If you pass a, b, c in that order. c, b, a would reverse the
        direction of the shift. You can also do arbitrary column orders
        like b, c, a.

        Args:
            *columns: A list of columns in the DataFrame.

        Returns: The DataFrame, with values shifted into nan cells per
            the order of the passed columns.

        """
        if len(columns) < 2:
            raise ValueError("Must supply at least 2 columns.")
        for i, c in enumerate(columns[:-1]):
            for c2 in columns[i + 1 :]:
                self.df[c].fillna(self.df[c2], inplace=True)
                self.df[c2] = np.where(self.df[c] == self.df[c2], np.nan, self.df[c2])
        return self.df

    @classmethod
    def from_file(cls, file_path: str, incl_header: bool = False, **kwargs):
        """
        Uses read_file to read in the passed file path.

        To read a Google Sheet, add .sheet as an extension to the file
        path.

        Args:
            file_path: The file path to the desired data file.
            incl_header: A boolean, indicates whether to include
                the unmodified header(if found) in an output tuple.
            kwargs: Kwargs will be passed to the reader function.

        Returns: For excel workbooks with multiple sheets, it will
            return a dictionary of sheet names as keys and raw
            sheet contents as values. For excel workbooks with
            one sheet and other file formats with a single set of
            data, it will return a Dataset object.

        """
        read_funcs = {
            ".xls": pd.read_excel,
            ".xlsx": pd.read_excel,
            ".csv": pd.read_csv,
            ".json": pd.read_json,
            ".sheet": text.from_gsheet,
            # file_paths with no extension are presumed to be dir_paths
            "": odbc.from_sqlite,
        }
        _, ext = os.path.splitext(file_path)
        # Expectation is that no column for these exts will have data
        # types that are safe for pandas to interpret.
        if ext in (".xls", ".xlsx", ".csv"):
            kwargs["dtype"] = object
        if ext not in read_funcs.keys():
            raise ValueError(
                f"read_file error: file extension must be one of "
                f"{read_funcs.keys()}"
            )
        else:
            df = u.purge_gap_rows(pd.DataFrame(read_funcs[ext](file_path, **kwargs)))
            df.columns, o_header = u.standardize_header(df.columns)
            if incl_header:
                return df, o_header
            else:
                return df

    def to_gsheet(self, sheet_name: str, **options):
        """
        Writes the DataFrame to a Google Sheet.
        Args:
            sheet_name: The desired name of the Google Sheet.
            **options: Key-value options to alter to_gsheet's behavior.
                Currently in use options:
                    sheet_title: The name of a sheet within the Google
                        Sheet to write to. Default is the first sheet.
                    s_api: An open SheetsAPI connection. If none is
                        passed, one will be created.
                    parent_folder: The name of the folder in Google
                        Drive that you want to save the Google Sheet
                        to.
                    drive_id: The id of the Shared Drive you want to
                        save the Google Sheet to.
                    metadata: A GeniusMetadata object. If passed, its
                        output_header will be used as the column header
                        in the output Google Sheet.
                    append: Set to True if you want to append values to
                        the end of an existing Google Sheet.

        Returns: The id of the newly created Google Sheet and its
            shape (columns are included as a row).

        """
        m = options.get("metadata")
        cols = m.output_header if m is not None else None
        return text.write_gsheet(
            sheet_name,
            self.df,
            sheet_title=options.get("sheet_title"),
            s_api=options.get("s_api"),
            columns=cols,
            parent_folder=options.get("parent_folder"),
            drive_id=options.get("drive_id"),
            append=options.get("append", False),
        )

    def to_sqlite(self, dir_path: str, table: str, **options):
        """
        Writes the DataFrame to a sqlite db.

        Args:
            dir_path: The directory path where the db file is/should
                be located.
            table: A string, the name of the table to enter data in.
            **options: Key-value options to alter to_sqlite's behavior.
                Currently in use options:
                    db_conn: An io.odbc.ODBConnector object if you have
                        one, otherwise to_sqlite will create it.
                    db_name: A string to specifically name the db to
                        output to. Default is 'datasets'
                    metadata: A GeniusMetadata object. If passed, its
                        contents will be saved to a table appended with
                        the names of its attributes.
                    drop_first: A boolean, if True, the target table
                        will be overwritten. True is the default.

        Returns: None

        """
        drop = options.get("drop_first", True)
        conn = odbc.quick_conn_setup(
            dir_path, options.get("db_name"), options.get("db_conn")
        )
        odbc.write_sqlite(conn, table, self.df, drop_first=drop)
        m = options.get("metadata")
        if m is not None:
            odbc.write_sqlite(conn, f"{table}_metadata", m.collected, drop_first=drop)
            if m.reject_ct > 0:
                odbc.write_sqlite(conn, f"{table}_rejects", m.rejects, drop_first=drop)

    @staticmethod
    def _order_transmutations(tms: (list, tuple)):
        """
        Places a list/tuple of transmutations in priority order.
        Primarily useful when mixing end-user custom transmutations
        with pre-built transmutations and the order they are executed
        in matters.

        Args:
            tms: A list/tuple of parser functions.

        Returns: The list of transmutations in decreasing priority
            order.

        """
        if len(tms) > 0:
            result = [tms[0]]
            if len(tms) > 1:
                for t in tms[1:]:
                    idx = -1
                    for j, r in enumerate(result):
                        if t.priority > r.priority:
                            idx = j
                            break
                    if idx >= 0:
                        result.insert(idx, t)
                    else:
                        result.append(t)
            return result
        else:
            return tms
