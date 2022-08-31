#!/usr/bin/env -S python -u

from argparse import ArgumentParser
from configparser import RawConfigParser
from contextlib import closing
from dataclasses import dataclass
from hashlib import md5
from operator import attrgetter
from os import fdatasync, isatty
from pathlib import Path
from shutil import get_terminal_size
from signal import SIGINT, SIGTERM, signal
from tarfile import TarFile
from time import ctime, monotonic
from typing import Iterator, List, Optional
from enum import Enum, unique

from requests import Session


@unique
class VerbosityLevel(Enum):
    AUTO = 'auto'
    QUIET = 'no'
    VERBOSE = 'yes'


class Printer:
    def __init__(self):
        self.__verbose = None

    def put(self, *args, **kwargs):
        if self.__verbose is None:
            raise RuntimeError('Verbosity level is not set.')
        if self.__verbose:
            print(*args, **kwargs)

    def is_verbose(self) -> bool:
        return self.__verbose

    def set_verbose(self, value: str):
        level = VerbosityLevel(value)
        if level == VerbosityLevel.AUTO:
            self.__verbose = isatty(1)
        elif level == VerbosityLevel.QUIET:
            self.__verbose = False
        elif level == VerbosityLevel.VERBOSE:
            self.__verbose = True
        else:
            raise ValueError(f'Not implemented value {level!r}:')


@dataclass(frozen=True)
class Package:
    path: Path
    size: int
    checksum: str


class Mirrors:
    def __init__(self, path: Path):
        parser = RawConfigParser()
        parser.read_string(path.read_text())
        norm = lambda value: value.strip().lower()
        self.__default = norm(parser.get('setup', 'default'))
        self.__mirrors = {norm(country): norm(url) for country, url in parser.items('mirrors')}

    def get(self, country: Optional[str]) -> str:
        return self.__mirrors[(country or self.__default).strip().lower()]

    def show(self):
        for country, url in self.__mirrors.items():
            print(f'{"*" if country == self.__default else " "}{country}: {url}')


ATTEMPTS = 2
CHUNK_SIZE = 2**20
BUF_SIZE = 32 * CHUNK_SIZE
DEF_ARCH = 'x86_64'
REPOS_CONF = {
    'aarch64': {
        'url': '{mirror}/{arch}/{branch}/{name}',
        'branches': ('core', 'extra', 'community', 'alarm'),
    },
    DEF_ARCH: {
        'url': '{mirror}/{branch}/os/{arch}/{name}',
        'branches': ('core', 'extra', 'community', 'multilib'),
    },
}

printer = Printer()


def _download(session: Session, url: str, file_path: Path, idx: int, amount: int):
    start_size = None
    start_time = None
    prev_percent = None
    term_cols, unused_rows = get_terminal_size(fallback=(128, 0))
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
    }

    def get_human_time(eta_time: float) -> str:
        secs = int(eta_time)
        mins, secs = divmod(secs, 60)
        hours, mins = divmod(mins, 60)
        days, hours = divmod(hours, 24)
        if days > 0:
            return f'{days}d{hours:02d}h{mins:02d}:{secs:02d}'
        return f'{hours:02d}:{mins:02d}:{secs:02d}'

    def show_progress(cur_size: int, total_size: int):
        nonlocal start_size
        nonlocal start_time
        nonlocal prev_percent

        cur_size /= 1024
        total_size /= 1024
        cur_time = monotonic()
        if (prev_percent is None) and (start_size is None) and (start_time is None):
            prev_percent = -1
            start_size = cur_size
            start_time = cur_time
        time_elapsed = cur_time - start_time
        speed = (cur_size - start_size) / time_elapsed if time_elapsed > 0 else 0
        eta_time = (total_size - cur_size) / speed if speed > 0 else 0
        speed *= 8
        if speed >= 1000:
            speed /= 1000
            speed_suffix = 'Mbps'
        else:
            speed_suffix = 'Kbps'
        cur_percent = int(cur_size / total_size * 100) if total_size > 0 else 0
        if prev_percent == cur_percent:
            return
        prev_percent = cur_percent
        size_delim = 1024 if total_size >= 1024 else 1
        size_suffix = 'Mb' if total_size >= 1024 else 'Kb'
        status = \
            f'\r[{idx}/{amount}] {file_path.name}: ' \
            f'{cur_size / size_delim:.2f}/{total_size / size_delim:.2f} {size_suffix} ({cur_percent}%) ' \
            f'@ {speed:.0f} {speed_suffix} ' \
            f'ETA {get_human_time(eta_time)}'
        tail = ' ' * (term_cols - len(status))
        printer.put(status + tail, end='')

    def fetch():
        with session.get(url, timeout=120, stream=True, headers=headers) as response:
            response.raise_for_status()
            cur_len = 0
            total_len = int(response.headers['Content-Length'])
            with file_path.open(mode='wb') as out_file:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    cur_len += len(chunk)
                    out_file.write(chunk)
                    show_progress(cur_len, total_len)
                out_file.flush()
                fdatasync(out_file.fileno())

    for unused_attempt in range(3):
        try:
            fetch()
            break
        except Exception as error:
            print(f"\nOops, error '{error!s}' occurred, will try again.")

    if not printer.is_verbose():
        printer.put(f'\r[{idx}/{amount}] {file_path.name}: done.')

    printer.put('')


def _download_files(work_dir: Path, mirror: str, arch: str, branch: str, names: List[str]):
    for item in work_dir.iterdir():
        if item.name in names:
            item.unlink()
    total = len(names)
    with Session() as session:
        for idx, name in enumerate(names, start=1):
            _download(session, REPOS_CONF[arch]['url'].format(mirror=mirror, arch=arch, branch=branch, name=name), work_dir / name, idx, total)


def _load_packages(work_dir: Path, branch: str) -> Iterator[Package]:
    print('>>> Loading packages descriptions from database...')
    with TarFile.open(Path(work_dir, f'{branch}.db.tar.gz')) as tar_file:
        for file_info in tar_file:
            if not file_info.isreg():
                continue
            if not file_info.name.endswith('desc'):
                continue
            prev_line, file_name, size, checksum = None, None, None, None
            with closing(tar_file.extractfile(file_info)) as desc_file:
                for line in desc_file:
                    cur_line = line.strip().decode('utf-8')
                    if prev_line == '%FILENAME%':
                        file_name = cur_line
                    elif prev_line == '%CSIZE%':
                        size = int(cur_line)
                    elif prev_line == '%MD5SUM%':
                        checksum = cur_line
                    prev_line = cur_line
            if all((file_name, size, checksum)):
                yield Package(path=Path(work_dir, file_name), size=size, checksum=checksum)
            else:
                raise RuntimeError(f'Invalid package description in {file_info.name!r}:')


def _check_packages(packages: List[Package]) -> List[Package]:
    result = []
    total = len(packages)
    broken_count = 0
    need_update_count = 0
    prefix = '\r>>> Checking packages'
    message = ''
    for i, package in enumerate(packages, start=1):
        message = f'{prefix}... {i}/{total}'
        if not package.path.is_file():
            need_update_count += 1
            result.append(package)
            printer.put(message, end='')
            continue
        md5_hash = md5()
        with package.path.open(mode='rb') as pkg_file:
            data = pkg_file.read(BUF_SIZE)
            while data:
                md5_hash.update(data)
                data = pkg_file.read(BUF_SIZE)
        if md5_hash.hexdigest() != package.checksum:
            broken_count += 1
            result.append(package)
        printer.put(message, end='')
    if broken_count or need_update_count:
        print(f'{prefix}: {need_update_count} need update, {broken_count} broken.')
    else:
        print('\r' + ' ' * len(message), end='')
        print(f'{prefix}: OK.')
    return result


def _remove_redundant_files(work_dir: Path, branch: str, packages_names: List[str]):
    service_files = (f'{branch}.db', f'{branch}.db.tar.gz', f'{branch}.files', f'{branch}.files.tar.gz')
    redundant_files = [
        item
        for item in work_dir.iterdir()
        if (item.name not in service_files) and (item.name not in packages_names)
    ]
    if redundant_files:
        print('>>> Removing redundant files...')
        for item in sorted(redundant_files):
            print(item.name)
            item.unlink()


def _fix_symlinks(work_dir: Path, branch: str):
    for link_ext, arc_ext in [('db', 'tar.gz'), ('files', 'tar.gz')]:
        link_path = Path(work_dir, f'{branch}.{link_ext}')
        target_name = f'{branch}.{link_ext}.{arc_ext}'
        if link_path.is_symlink():
            try:
                target_path = link_path.resolve()
            except (FileNotFoundError, RuntimeError):
                link_path.unlink()
            else:
                if target_path != Path(work_dir, target_name):
                    link_path.unlink()
        if not link_path.is_symlink():
            if link_path.exists():
                link_path.unlink()
            print(f'>>> Fixing symlink {link_path.name!r}...')
            link_path.symlink_to(target_name)


def main():
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--config', type=Path, default=Path('~/.config/armi.conf'), help='Config file.')
    arg_parser.add_argument('-d', '--destination-dir', type=Path, default=Path().cwd().resolve(), help='Destination directory.')
    arg_parser.add_argument('-m', '--mirror', default=None, help='Mirror URL to download from.')
    arg_parser.add_argument('-l', '--list', dest='show_list', action='store_true', help='List all configured mirrors.')
    arg_parser.add_argument('-A', '--arch', dest='arches', choices=list(REPOS_CONF) + ['all'], nargs='+', default=[DEF_ARCH], help='Arches to sync.')
    arg_parser.add_argument('-v', '--verbose', choices=list(item.value for item in VerbosityLevel), default=VerbosityLevel.AUTO.value, help='Be verbose.')
    args = arg_parser.parse_args()

    mirrors = Mirrors(args.config.expanduser())
    if args.show_list:
        mirrors.show()
        return

    for signo in (SIGINT, SIGTERM):
        signal(signo, lambda *unused_args: exit(1))

    printer.set_verbose(args.verbose)

    errors = False
    mirror = mirrors.get(args.mirror)
    arches = list(REPOS_CONF) if 'all' in args.arches else args.arches
    print(f'>>> Using mirror {mirror!r} and arches: {", ".join(arches)}.')

    for arch in arches:
        print(f'>>> Arch: {arch!r}.')
        for branch in REPOS_CONF[arch]['branches']:
            work_dir = Path(args.destination_dir, arch, branch)
            work_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
            print(f'>>> Syncing branch {branch!r}.')
            _download_files(work_dir, mirror, arch, branch, [f'{branch}.db.tar.gz', f'{branch}.files.tar.gz'])
            packages = sorted(_load_packages(work_dir, branch), key=attrgetter('size'), reverse=True)
            target_packages = []
            for attempt in range(ATTEMPTS):
                target_packages = _check_packages(target_packages or packages)
                if not target_packages:
                    _remove_redundant_files(work_dir, branch, [package.path.name for package in packages])
                    _fix_symlinks(work_dir, branch)
                    print(f'>>> Branch {branch!r} has been synced successfully.')
                    break
                print(f'>>> Downloading packages...')
                _download_files(work_dir, mirror, arch, branch, [package.path.name for package in target_packages])
            else:
                errors = True
                print('>>> Previous attempt has been failed, trying one more time.')

    if errors:
        print('ERROR: Some branches have been failed to sync.')
        exit(1)

    for arch in arches:
        Path(args.destination_dir, f'last_update.{arch}').write_text(f'{ctime()}\n')


if __name__ == '__main__':
    main()
