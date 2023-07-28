import argparse
import hashlib
import logging
import os.path
import shutil
import sys
import time


class PathMetadata:
    def __init__(self, path: str):
        self._path = path

        stats = os.stat(path, follow_symlinks=False)
        self._mtime = stats.st_mtime

        self._accessed = True

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return os.stat(self._path, follow_symlinks=False).st_size

    @property
    def mtime(self) -> float:
        return self._mtime

    def update_mtime(self) -> bool:
        stats = os.stat(self._path, follow_symlinks=False)
        new_mtime = stats.st_mtime
        if new_mtime != self.mtime:
            self._mtime = new_mtime
            return True
        return False

    def md5(self) -> bytes:
        md5_hash = hashlib.md5()
        with open(self._path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)
        return md5_hash.digest()

    @property
    def accessed(self) -> bool:
        return self._accessed

    @accessed.setter
    def accessed(self, value: bool):
        self._accessed = value

    def is_link(self) -> bool:
        return os.path.islink(self._path)

    def read_link(self) -> str:
        return os.readlink(self._path)


class MetadataCache:
    def __init__(self):
        self._cache: dict[str, PathMetadata] = {}

    def access(self, path: str) -> PathMetadata:
        metadata = self._cache.get(path)
        if metadata is not None:
            metadata.accessed = True
        return metadata

    def add_path(self, path: PathMetadata):
        self._cache[path.path] = path

    def prune(self):
        """Removes metadata that hasn't been accessed since last pruning"""
        self._cache = {path: metadata for path, metadata in self._cache.items() if metadata.accessed}
        for _, metadata in self._cache.items():
            metadata.accessed = False


class TouchedDirectories:
    """Keeps track of directories whose content has been modified and corrects their modification time to match the
    modification time in the source directory"""

    def __init__(self, source: str, replica: str):
        self._source = source
        self._replica = replica
        self._directories = {}

    def add_directory(self, replica_path: str):
        common = os.path.commonpath([replica_path, self._replica])
        replica_path = replica_path.lstrip(common)

        components = []
        while True:
            replica_path, directory = os.path.split(replica_path)
            if directory == '':
                break
            components.insert(0, directory)

        directory = self._directories
        for component in components:
            directory = directory.setdefault(component, {})

    def fix_metadata(self):
        def recurse_fix(source_path, replica_path, directories):
            for directory, sub_dirs in directories.items():
                source_path = os.path.join(source_path, directory)
                replica_path = os.path.join(replica_path, directory)
                recurse_fix(source_path, replica_path, sub_dirs)
                logging.debug(f'Fixing directory "{replica_path}" metadata with "{source_path}"')
                shutil.copystat(source_path, replica_path)

        recurse_fix(self._source, self._replica, self._directories)

        self._directories = {}


def main():
    source, replica, log_file, log_level, interval = parse_arguments()

    setup_logging(log_file, log_level)

    initial_checks(replica, source, log_file)

    metadata_cache = MetadataCache()

    while True:
        sync_start = time.monotonic()
        perform_sync(source, replica, metadata_cache)
        metadata_cache.prune()
        sync_length = time.monotonic() - sync_start
        sleep_length = interval - sync_length

        skips = 0
        while sleep_length < 0:
            sleep_length += interval
            skips += 1

        if skips:
            logging.warning(f'{skips} sync passes have been skipped due to sync taking too long')

        time.sleep(interval)


def parse_arguments():
    def parse_time(string) -> int:
        number = int(string[:-1])
        unit = string[-1]
        match unit:
            case 's':
                return number
            case 'm':
                return number * 60
            case _:
                raise argparse.ArgumentTypeError

    parser = argparse.ArgumentParser(prog='sync', description='Program for synchronizing and replicating source '
                                                              'directory into a replica directory.')

    parser.add_argument('-s', '--source', help='Source directory path', required=True)
    parser.add_argument('-r', '--replica', help='Replicated directory path', required=True)
    parser.add_argument('-lf', '--log-file', help='Log file path', default='')
    parser.add_argument('-ll', '--log-level', help='Defaults to INFO', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument('-i', '--interval', help='Sync interval in seconds or minutes (defaults to 1m)',
                        type=parse_time, default='1m')

    args = parser.parse_args()
    log_level = logging.getLevelName(args.log_level)
    return args.source, args.replica, args.log_file, log_level, args.interval


def setup_logging(log_file: str, log_level):
    log_handlers = [logging.StreamHandler(sys.stdout)]
    log_file_ok = True
    if log_file:
        if os.path.isfile(log_file) and not os.access(log_file, os.W_OK):
            log_file_ok = False
        else:
            log_handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(level=log_level, format='[%(levelname)s]:%(asctime)s:%(message)s', handlers=log_handlers)
    if not log_file_ok:
        logging.warning(f'Log file "{log_file}" cannot be written to')


def initial_checks(replica: str, source: str, log_file: str):
    if not os.path.isdir(source):
        logging.critical(f'Source: "{source}" is not a directory')
        raise Exception(f'Source: "{source}" is not a directory')
    if not os.path.isdir(replica):
        logging.critical(f'Replica: "{replica}" is not a directory')
        raise Exception(f'Replica: "{replica}" is not a directory')

    if not os.access(source, os.R_OK | os.X_OK):
        logging.critical(f'Source directory "{source}" has insufficient permissions')
        raise Exception(f'Source directory "{source}" has insufficient permissions')
    if not os.access(replica, os.W_OK | os.X_OK):
        logging.critical(f'Replica directory "{replica}" has insufficient permissions')
        raise Exception(f'Replica directory "{replica}" has insufficient permissions')

    log_file_realpath = os.path.realpath(log_file)
    if log_file_realpath.startswith(os.path.realpath(source)):
        logging.warning(f'Log file "{log_file}" is inside source directory "{source}"')
    if log_file_realpath.startswith(os.path.realpath(replica)):
        logging.warning(f'Log file "{log_file}" is inside replica directory "{replica}"')


def perform_sync(source: str, replica: str, cache: MetadataCache):
    logging.info('Beginning synchronization pass')

    touched_dirs = TouchedDirectories(source, replica)

    for (s_path, s_dirs, s_files), (r_path, r_dirs, r_files) in zip(os.walk(source), os.walk(replica)):
        s_dirs.sort()
        s_files.sort()
        r_dirs.sort()
        r_files.sort()

        handle_directories(s_path, s_dirs, r_path, r_dirs, cache)

        handle_files(s_path, s_files, r_path, r_files, cache, touched_dirs)

    touched_dirs.fix_metadata()

    logging.info('Synchronization pass complete')


def handle_directories(s_path: str, s_dirs: [str], r_path: str, r_dirs: [str], cache: MetadataCache):
    # this loop goes over source and replica directories, finds and copies missing directories, removes extra
    # directories, and makes sure that directories that are already identical won't be searched further
    i = 0
    while i < len(s_dirs):
        s_dir = s_dirs[i]
        source_dir_path = os.path.join(s_path, s_dir)

        if len(r_dirs) <= i:
            # when there are source directories and no more replica directories, all source directories are copied to
            # replica, further search skipped
            replica_dir_path = os.path.join(r_path, s_dir)
            copy_directory_tree(source_dir_path, replica_dir_path, cache)
            del s_dirs[i]
            continue
        else:
            r_dir = r_dirs[i]
            replica_dir_path = os.path.join(r_path, r_dir)

            if s_dir > r_dir:
                # removing extra directories
                remove_directory_tree(replica_dir_path)
                del r_dirs[i]
                continue

            if s_dir < r_dir:
                # copying missing directories, further search skipped
                replica_dir_path = os.path.join(r_path, s_dir)
                copy_directory_tree(source_dir_path, replica_dir_path, cache)
                del s_dirs[i]
                continue

            # when both source and replica have the same directory
            metadata = cache.access(source_dir_path)
            if metadata is None:
                # source directory has been encountered for the first time
                cache.add_path(PathMetadata(source_dir_path))
            else:
                if not metadata.update_mtime() and metadata.mtime == os.stat(replica_dir_path).st_mtime:
                    # when directory hasn't been modified since last sync pass search is skipped
                    logging.debug(f'Skipping "{replica_dir_path}", no change')
                    del s_dirs[i]
                    del r_dirs[i]
                    continue

            if not os.access(replica_dir_path, os.W_OK | os.X_OK):
                logging.warning(f'Skipping replica directory "{replica_dir_path}" due to insufficient permissions')
                del s_dirs[i]
                del r_dirs[i]
                continue
        i += 1

    # any extra replica directories get removed
    for r_dir in r_dirs[i:]:
        replica_dir_path = os.path.join(r_path, r_dir)
        remove_directory_tree(replica_dir_path)
    del r_dirs[i:]


def copy_directory_tree(source_dir_path: str, replica_dir_path: str, cache: MetadataCache):
    logging.info(f'Copying directory "{source_dir_path}" to "{replica_dir_path}" in replica')
    shutil.copytree(source_dir_path, replica_dir_path, symlinks=True)
    cache.add_path(PathMetadata(source_dir_path))


def remove_directory_tree(replica_dir_path: str):
    logging.info(f'Removing directory "{replica_dir_path}" from replica')
    if os.path.islink(replica_dir_path):
        os.remove(replica_dir_path)
    else:
        shutil.rmtree(replica_dir_path)


def handle_files(s_path: str, s_files: [str], r_path: str, r_files: [str], cache: MetadataCache,
                 touched_dirs: TouchedDirectories):
    # this works largely the same as handling directories
    i = 0
    while i < len(s_files):
        s_file = s_files[i]
        source_file_path = os.path.join(s_path, s_file)

        if len(r_files) <= i:
            replica_file_path = os.path.join(r_path, s_file)
            copy_file(source_file_path, replica_file_path, cache, touched_dirs)
            del s_files[i]
            continue
        else:
            r_file = r_files[i]
            replica_file_path = os.path.join(r_path, r_file)

            if s_file > r_file:
                remove_file(replica_file_path, touched_dirs)
                del r_files[i]
                continue

            if s_file < r_file:
                replica_file_path = os.path.join(r_path, s_file)
                copy_file(source_file_path, replica_file_path, cache, touched_dirs)
                del s_files[i]
                continue

            compare_files(source_file_path, replica_file_path, cache, touched_dirs)
        i += 1

    for r_file in r_files[i:]:
        replica_file_path = os.path.join(r_path, r_file)
        remove_file(replica_file_path, touched_dirs)


def copy_file(source_file_path: str, replica_file_path: str, cache: MetadataCache, touched_dirs: TouchedDirectories):
    logging.info(f'Copying file "{source_file_path}" to "{replica_file_path}" in replica')
    shutil.copy2(source_file_path, replica_file_path, follow_symlinks=False)
    cache.add_path(PathMetadata(source_file_path))
    touched_dirs.add_directory(os.path.dirname(replica_file_path))


def remove_file(replica_file_path: str, touched_dirs: TouchedDirectories):
    logging.info(f'Removing file "{replica_file_path}" from replica')
    os.remove(replica_file_path)
    touched_dirs.add_directory(os.path.dirname(replica_file_path))


def compare_files(source_file_path: str, replica_file_path: str, cache: MetadataCache,
                  touched_dirs: TouchedDirectories):
    source_metadata = cache.access(source_file_path)
    replica_metadata = PathMetadata(replica_file_path)

    first_encounter = False
    if source_metadata is None:
        first_encounter = True
        source_metadata = PathMetadata(source_file_path)
        cache.add_path(source_metadata)

    if (source_metadata.is_link() and replica_metadata.is_link()
            and source_metadata.read_link() == replica_metadata.read_link()):
        # if source and replica files are symlinks pointing to the same file, updating is not necessary
        logging.debug(f'Skipping link "{replica_file_path}", no change')
        return

    if not first_encounter:
        if not source_metadata.update_mtime() and source_metadata.mtime == replica_metadata.mtime:
            # when the file has been encountered in previous sync pass and there have been no modifications, updating
            # is not necessary
            logging.debug(f'Skipping "{replica_file_path}", no change')
            return

    if source_metadata.size == replica_metadata.size and source_metadata.md5() == replica_metadata.md5():
        # file contents are the same, updating is not necessary
        logging.debug(f'Skipping "{replica_file_path}", contents identical')
        if not first_encounter:
            shutil.copystat(source_file_path, replica_file_path)
            touched_dirs.add_directory(os.path.dirname(replica_file_path))
        return

    update_file(source_file_path, replica_file_path, touched_dirs)


def update_file(source_file_path: str, replica_file_path: str, touched_dirs: TouchedDirectories):
    logging.info(f'Updating replica file "{replica_file_path}" with source file "{source_file_path}"')
    os.remove(replica_file_path)
    shutil.copy2(source_file_path, replica_file_path, follow_symlinks=False)
    touched_dirs.add_directory(os.path.dirname(replica_file_path))


if __name__ == '__main__':
    main()
