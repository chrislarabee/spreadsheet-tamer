import os
from importlib import import_module


def gather_prebuilt_transmutations(lib) -> dict:
    mods = []
    for m in lib:
        mods.append(('datagenius.lib', m))
    return _collect_tms(mods)


def gather_custom_transmutations(cwd) -> dict:
    """
    Collects all functions decorated as transmutations in the passed
    directory, as long as it is a git repository. These are then
    automatically added to the appropriate pipeline stage in
    datagenius.genius when it is imported.

    Returns: A dictionary containing transmutation stage names as keys
        and a list of transmutations corresponding to that stage as
        values.

    """
    tms_by_stage = {}
    if os.path.exists(os.path.join(cwd, '.git')):
        p = os.path.join(cwd, '.gitignore')
        if os.path.exists(p):
            with open(p, 'r') as r:
                g = [line.strip() for line in r]
        g += ['.git', 'datagenius']
        dirs = _get_repository_dirs(cwd, g)
        mods = _get_modules(dirs, g)
        tms_by_stage = _collect_tms(mods)
    return tms_by_stage


def _collect_tms(mods):
    tms_by_stage = dict()
    for m in mods:
        tms = _get_tms(m)
        for t in tms:
            if t.stage not in tms_by_stage.keys():
                tms_by_stage[t.stage] = []
            tms_by_stage[t.stage].append(t)
    return tms_by_stage


def _get_repository_dirs(cwd: str, ignore: list) -> list:
    """
    Gets all top-level directories in the cwd/ Directories in the
    passed ignore list will be ignored.

    Args:
        cwd: The path to a root directory to search.
        ignore: A list of strings, the names of directories to ignore.

    Returns: A list of the qualifying directories.

    """
    results = []
    for f in os.listdir(cwd):
        if os.path.isdir(f) and f not in ignore and 'test' not in f:
            results.append(f)
    return results


def _get_modules(dirs: list, ignore: list):
    """
    Gets all python modules in the passed list of directories. Any
    files in directories listed in ignore or files listed in ignore
    will be ignored.

    Args:
        dirs: A list of strings, the dirs to collect modules from.
        ignore: A list of strings, the names of files and
            subdirectories in dirs to ignore.

    Returns: A list of qualifying python modules.

    """
    results = set()
    for d in dirs:
        for root, _, files in os.walk(d):
            _, root_dir = os.path.split(root)
            if root_dir not in ignore:
                for f in files:
                    m, ext = os.path.splitext(f)
                    if m not in ignore and ext == '.py':
                        root = root.replace('\\', '.')
                        results.add((root, m))
    return list(results)


def _get_tms(module: tuple) -> list:
    """
    Imports the passed module locally and checks its objects to see if
    they are functions decorated as transmutations.

    Args:
        module: A tuple of the module name and its parent package(s).
            e.g. ('clean', 'datagenius.lib')

    Returns: A list of the transmutation functions found in the module.

    """
    results = []
    m = import_module('.' + module[1], module[0])
    for f_obj in m.__dict__.values():
        if hasattr(f_obj, 'is_transmutation'):
            results.append(f_obj)
    return results
