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
        if self.df.columns[0] in ('Unnamed: 0', 0):
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

    @classmethod
    def from_file(cls, file_path: str, **kwargs):
        """
        Uses read_file to read in the passed file path.

        Args:
            file_path: The file path to the desired data file.

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
            kwargs['dtype'] = 'object'
        if ext not in read_funcs.keys():
            raise ValueError(f'read_file error: file extension must be '
                             f'one of {read_funcs.keys()}')
        else:
            df = u.purge_gap_rows(
                pd.DataFrame(read_funcs[ext](file_path, **kwargs))
            )
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


class Supplement:
    """
    A callable object designed to combine arbitrary numbers of pandas
    DataFrames via exact and inexact methods. Designed to handle
    complex merges with some rows in a dataset being joined in one way
    and other rows being joined in different ways.
    """
    def __init__(self, *on, select_cols: (str, tuple) = None):
        """

        Args:
            *on: An arbitrary list of column names, tuples of column
                names and dictionary conditions, or MatchRule objects.
                All columns referenced must be in the DataFrames that
                will be passed to Supplement().
            select_cols: A list of column names in the secondary
                DataFrames that you want to include in the results.
                Useful if you only want some of the columns in the
                secondary DataFrames.

        """
        self.select: (tuple, None) = u.tuplify(select_cols)
        self.plan = self.build_plan(on)

    @staticmethod
    def do_exact(df1: pd.DataFrame, df2: pd.DataFrame, on: tuple,
                 rsuffix: str = '_s') -> pd.DataFrame:
        """
        Merges two DataFrames with overlapping columns based on exact
        matches in those columns.

        Args:
            df1: A pandas DataFrame.
            df2: A pandas DataFrame containing columns shared with df1.
            on: A tuple of columns shared by df1 and df2, which will be
                used to left join rows from df2 onto exact matches in
                df1.
            rsuffix: An optional suffix to use for overlapping columns
                outside the on columns. Will only be applied to df2
                columns.

        Returns: A DataFrame containing all the rows in df1, joined
            with any matched rows from df2.

        """
        return df1.merge(
            df2,
            'left',
            on=on,
            suffixes=('', rsuffix)
        )

    @staticmethod
    def do_inexact(df1: pd.DataFrame, df2: pd.DataFrame, on: tuple,
                   thresholds: tuple, block: tuple = None,
                   rsuffix: str = '_s') -> pd.DataFrame:
        """

        Args:
            df1: A pandas DataFrame.
            df2: A pandas DataFrame containing columns shared with df1.
            on: A tuple of columns shared by df1 and df2, which will be
                used to left join rows from df2 onto inexact matches in
                df1.
            thresholds: A tuple of floats, indicating how close each on
                comparison must be to qualify the row as a match. Must
                be the same length as on.
            block: A tuple of columns shared by df1 and df2, similar to
                on, which must represent an exact match between the two
                frames. Useful when you can reduce the possible match
                space of two datasets by restricting inexact matches to
                records that at least have an exact match on a different
                column.
            rsuffix: An optional suffix to use for overlapping columns
                outside the on columns. Will only be applied to df2
                columns.

        Returns: A DataFrame containing all the rows in df1, joined
            with any matched rows from df2.

        """
        # The recordlinkage library is currently passing an argument
        # to the underlying jellyfish library that jellyfish is going
        # to deprecate eventually. Nothing we can do about that so just
        # suppress it:
        warnings.filterwarnings(
            'ignore', message="the name 'jaro_winkler'",
            category=DeprecationWarning)
        idxr = link.Index()
        idxr.block(block) if block is not None else idxr.full()
        candidate_links = idxr.index(df1, df2)
        compare = link.Compare()
        # Create copies since contents of the Dataframe need to
        # be changed.
        frames = (df1.copy(), df2.copy())

        for i, o in enumerate(on):
            compare.string(
                o, o, method='jarowinkler', threshold=thresholds[i])
            # Any columns containing strings should be lowercase to
            # improve matching:
            for f in frames:
                if f.dtypes[o] == 'O':
                    f[o] = f[o].astype(str).str.lower()

        features = compare.compute(candidate_links, *frames)
        matches = features[features.sum(axis=1) == len(on)].reset_index()

        a = matches.join(df1, on='level_0', how='outer', rsuffix='')
        b = a.join(df2, on='level_1', how='left', rsuffix=rsuffix)
        drop_cols = ['level_0', 'level_1', *[i for i in range(len(on))]]
        b.drop(columns=drop_cols, inplace=True)
        return b

    @staticmethod
    def chunk_dframes(plan: tuple, *frames) -> tuple:
        """
        Takes any number of pandas DataFrames and breaks each one into
        chunks based on a chunking plan of MatchRule objects.

        Args:
            plan: A tuple of MatchRule objects created by
                Supplement.build_plan, which will be used to chunk each
                DataFrame.
            *frames: An arbitrary number of pandas DataFrames, each of
                which must have the column labels named in the plan.

        Returns: Plan, with each MatchRule in the plan now having the
            chunk of rows that match its conditions, and the first
            DataFrame from frames, which contains any remaining rows
            that didn't match any of the conditions.

        """
        df1 = frames[0]
        for i, df in enumerate(frames):
            for p in plan:
                conditions = p.output('conditions')
                match, result = Supplement.slice_dframe(df, conditions)
                p.append(match)
                if result:
                    df.drop(match.index, inplace=True)
        return plan, df1

    @staticmethod
    def slice_dframe(df: pd.DataFrame, conditions: dict) -> tuple:
        """
        Takes a dictionary of conditions in the form of:
            {'column_label': tuple(of, values, to, match)
        and returns a dataframe that contains only the rows that match
        all the passed conditions.

        Args:
            df: A pandas Dataframe containing the column_labels in
                conditions.keys()
            conditions: A dictionary of paired column_labels and tuples
                of values to match against.

        Returns: A DataFrame containing only the matching rows and a
            boolean indicating whether matching rows were found or if
            the DataFrame is simply being returned untouched.

        """
        df = df.copy()
        row_ct = df.shape[0]
        no_conditions = True
        for k, v in conditions.items():
            if k is not None:
                no_conditions = False
                df = df[df[k].isin(v)]
        new_ct = df.shape[0]
        result = True if (row_ct >= new_ct != 0
                          or no_conditions) else False
        return df, result

    @staticmethod
    def build_plan(on: tuple) -> tuple:
        """
        Takes a tuple of mixed simple and complex on values and ensures
        they are standardized in the ways that chunk_dframes expects.

        Args:
            on: A tuple containing simple strings, tuples of
                dictionary and string/tuple pairs, or Match Rule
                objects.

        Returns: A tuple of MatchRule objects, one for each complex on
            and a single MatchRule for the simple ons at the end.

        """
        simple_ons = list()
        complex_ons = list()
        for o in on:
            if isinstance(o, e.MatchRule):
                complex_ons.append(o)
            elif isinstance(o, str):
                simple_ons.append(o)
            elif isinstance(o, tuple):
                pair = [None, None]
                for oi in o:
                    if isinstance(oi, dict):
                        pair[1] = oi
                    elif isinstance(oi, (str, tuple)):
                        pair[0] = u.tuplify(oi)
                    else:
                        raise ValueError(
                            f'tuple ons must have a dict as one of '
                            f'their arguments and a str/tuple as the '
                            f'other Invalid tuple={o}'
                        )
                mr = e.MatchRule(*pair[0], conditions=pair[1])
                complex_ons.append(mr)
        if len(simple_ons) > 0:
            complex_ons.append(e.MatchRule(*simple_ons))
        return tuple(complex_ons)

    def __call__(self, *frames,
                 suffixes: (str, tuple) = None,
                 split_results: bool = False) -> (tuple, pd.DataFrame):
        """
        Executes the plan established by instantiation of Supplement
        on the passed dataframes.

        Args:
            *frames: An arbitrary number of DataFrames. The first frame
                will be treated as the primary frame.
            suffixes: A string or tuple of strings, the suffixes you
                would like to append to columns in the secondary frames
                that have overlapping column names in the other frames.
                If passed, you must pass as many suffixes as the length
                of frames - 1.
            split_results: A boolean, set to True if you want to return
                two DataFrames, one which contains the rows from the
                primary DataFrame and the successfully matched rows
                from the subsequent dataframes. Otherwise, will return
                a single dataframe containing all the rows in the
                primary frame with the successfully matched rows joined
                onto it.

        Returns: A tuple of DataFrames or a single DataFrame.

        """
        chunks, remainder = self.chunk_dframes(self.plan, *frames)
        results = []
        if suffixes is None:
            suffixes = tuple(
                ['_' + a for a in u.gen_alpha_keys(len(frames) - 1)])
        else:
            suffixes = u.tuplify(suffixes)
        if len(suffixes) != len(frames) - 1:
            raise ValueError(f'Length of suffixes must be equal to the'
                             f'number of frames passed - 1. Suffix len='
                             f'{len(suffixes)}, suffixes={suffixes}')
        p_cols = set(frames[0].columns)
        for mr in chunks:
            p_frame = mr.chunks[0]
            o_frames = mr.chunks[1:]
            for i, other in enumerate(o_frames):
                rsuffix = suffixes[i]
                if not other.empty:
                    o_cols = set(other.columns)
                    other['merged_on'] = ','.join(mr.on)
                    other = (
                        other[{
                            *mr.on, *o_cols.intersection(set(self.select)),
                            'merged_on'
                        }] if self.select else other
                    )
                    if mr.inexact:
                        p_frame = self.do_inexact(
                            p_frame, other, mr.on,
                            mr.thresholds, mr.block, rsuffix
                        )
                    else:
                        p_frame = self.do_exact(
                            p_frame, other, mr.on, rsuffix
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
