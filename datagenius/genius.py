import os
import warnings
from typing import Callable

import pandas as pd
import recordlinkage as link

import datagenius.lib as lib
import datagenius.element as e
import datagenius.util as u
import datagenius.metadata as md
from datagenius.io import odbc


@pd.api.extensions.register_dataframe_accessor('genius')
class GeniusAccessor:
    """
    A custom pandas DataFrame accessor that adds a number of additional
    methods and properties that extend the DataFrame's functionality.
    """
    @property
    def preprocess_tms(self, header_func=lib.preprocess.detect_header):
        pp_tms = [
            lib.preprocess.normalize_whitespace,
        ]
        if self.df.columns[0] in ('Unnamed: 0', 0):
            pp_tms.insert(0, lib.preprocess.purge_pre_header)
            pp_tms.insert(0, header_func)
        return pp_tms

    def __init__(self, df: pd.DataFrame):
        """

        Args:
            df: A pandas DataFrame.
        """
        self.df = df

    def preprocess(
            self,
            header_func: Callable = lib.preprocess.detect_header,
            **options) -> tuple:
        """
        A convenient way to run functions from lib.preprocess on
        self.df.

        Args:
            header_func: A callable object that takes a Dataset object
                and kwargs.
            **options: Keyword args. See the preprocess transmutations
                for details on the arguments they take.
        Returns: self.df, modified by preprocess transmutations, and a
            metadata dictionary describing the changes made.

        """
        pp_tms = [
            lib.preprocess.normalize_whitespace,
        ]
        if self.df.columns[0] in ('Unnamed: 0', 'unnamed:_0', 0):
            pp_tms.insert(0, lib.preprocess.purge_pre_header)
            pp_tms.insert(0, header_func)
        return self.transmute(
            pp_tms,
            **options
        )
    
    def explore(self, metadata: md.GeniusMetadata = None) -> tuple:
        """
        A convenient way to run functions from lib.explore on self.df.

        Args:
            metadata: A GeniusMetadata object.

        Returns: self.df, and a metadata dictionary describing explore
            results.

        """
        ex_tms = [
            lib.explore.count_uniques,
            lib.explore.count_nulls,
            lib.explore.collect_data_types
        ]
        return self.transmute(
            ex_tms,
            metadata
        )

    def clean(
            self,
            metadata: md.GeniusMetadata = None,
            **options) -> tuple:
        """
        A convenient way to run functions from lib.clean on self.df.

        Args:
            metadata: A GeniusMetadata object.
            **options: Keyword args. See the clean transmutations
                for details on the arguments they take.

        Returns: self.df, modified by clean transmutations, and a
            metadata dictionary describing the changes made.

        """
        all_cl_tms = [
            lib.clean.complete_clusters,
            lib.clean.reject_incomplete_rows,
            lib.clean.reject_on_conditions,
            lib.clean.reject_on_str_content,
        ]
        cl_tms = self._align_tms_with_options(all_cl_tms, options)
        return self.transmute(cl_tms, metadata, **options)

    def reformat(
            self,
            metadata: md.GeniusMetadata = None,
            **options) -> tuple:
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
        all_rf_tms = [
            lib.reformat.reformat_df,
            lib.reformat.fill_defaults,
        ]
        rf_tms = self._align_tms_with_options(all_rf_tms, options)
        return self.transmute(rf_tms, metadata, **options)

    def standardize(
            self,
            metadata: md.GeniusMetadata = None,
            **options) -> tuple:
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
        all_st_tms = [
            lib.clean.cleanse_typos,
            lib.clean.convert_types,
        ]
        st_tms = self._align_tms_with_options(all_st_tms, options)
        return self.transmute(st_tms, metadata, **options)

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
            if None not in u.align_args(tm, options, 'df').values():
                result.append(tm)
        return result

    def transmute(
            self,
            transmutations: list,
            metadata: md.GeniusMetadata = None,
            **options) -> tuple:
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
        transmutations = self.order_transmutations(transmutations)
        self.df = metadata(self.df, *transmutations, **options)
        return self.df, metadata

    def supplement(
            self,
            *other,
            on: (str, list, tuple),
            select_cols: (str, tuple) = None,
            suffixes: (str, tuple) = None,
            split_results: bool = False) -> (pd.DataFrame, tuple):
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
            lib.supplement.build_plan(ons), *frames)
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
                    other['merged_on'] = ','.join(sg.on)
                    other = (
                        other[{
                            *sg.on,
                            *o_cols.intersection(set(select_cols)),
                            'merged_on'
                        }] if select_cols else other
                    )
                    if sg.inexact:
                        p_frame = lib.supplement.do_inexact(
                            p_frame, other, sg.on,
                            sg.thresholds, sg.block, rsuffix
                        )
                    else:
                        p_frame = lib.supplement.do_exact(
                            p_frame, other, sg.on, rsuffix
                        )
            results.append(p_frame)
        result_df = pd.concat(results)
        unmatched = result_df[result_df['merged_on'].isna()]
        matched = result_df[~result_df['merged_on'].isna()]
        unmatched = pd.concat([unmatched[p_cols], remainder])
        if split_results:
            return matched, unmatched
        else:
            return pd.concat([matched, unmatched])

    @classmethod
    def from_file(
            cls,
            file_path: str,
            incl_header: bool = False,
            **kwargs):
        """
        Uses read_file to read in the passed file path.

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
            '.xls': pd.read_excel,
            '.xlsx': pd.read_excel,
            '.csv': pd.read_csv,
            '.json': pd.read_json,
            # file_paths with no extension are presumed to be dir_paths
            '': odbc.from_sqlite
        }
        _, ext = os.path.splitext(file_path)
        # Expectation is that no column for these exts will have data
        # types that are safe for pandas to interpret.
        if ext in ('.xls', '.xlsx', '.csv'):
            kwargs['dtype'] = object
        if ext not in read_funcs.keys():
            raise ValueError(f'read_file error: file extension must be '
                             f'one of {read_funcs.keys()}')
        else:
            df = u.purge_gap_rows(
                pd.DataFrame(read_funcs[ext](file_path, **kwargs))
            )
            df.columns, o_header = u.standardize_header(df.columns)
            if incl_header:
                return df, o_header
            else:
                return df

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

        Returns: None

        """
        conn = odbc.quick_conn_setup(
            dir_path,
            options.get('db_name'),
            options.get('db_conn')
        )
        odbc.write_sqlite(conn, table, self.df)
        m = options.get('metadata')
        if m is not None:
            odbc.write_sqlite(conn, f'{table}_metadata', m.collected)
            if m.reject_ct > 0:
                odbc.write_sqlite(conn, f'{table}_rejects', m.rejects)

    @staticmethod
    def order_transmutations(tms: (list, tuple)):
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
