#!/usr/bin/python

from argparse import ArgumentParser
from contextlib import closing
from dataclasses import dataclass
from hashlib import md5
from operator import attrgetter
from os import fdatasync
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from tarfile import TarFile
from time import ctime, monotonic
from typing import Iterator, List

from requests import Session


@dataclass(frozen=True)
class Package:
    path: Path
    size: int
    checksum: str


ATTEMPTS = 2
CHUNK_SIZE = 2**20
BUF_SIZE = 32 * CHUNK_SIZE
BRANCHES = ('core', 'extra', 'community', 'multilib')


def _download(session: Session, url: str, file_path: Path, idx: int, amount: int):
    start_size = None
    start_time = None
    prev_percent = None

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
        speed = (cur_size - start_size) * 8 / time_elapsed if time_elapsed > 0 else 0
        if speed >= 1000:
            speed /= 1000
            speed_suffix = 'Mbps'
        else:
            speed_suffix = 'Kbps'
        eta_time = (total_size - cur_size) / speed if speed > 0 else 0
        cur_percent = int(cur_size / total_size * 100) if total_size > 0 else 0
        if prev_percent == cur_percent:
            return
        prev_percent = cur_percent
        size_delim = 1024 if total_size >= 1024 else 1
        size_suffix = 'Mb' if total_size >= 1024 else 'Kb'
        status = \
            f'\r[{idx}/{amount}] {file_path.name}: ' \
            f'{cur_size / size_delim:.2f}/{total_size / size_delim:.2f} {size_suffix} ({cur_percent}%) ' \
            f'@ {speed:.3f} {speed_suffix} ' \
            f'ETA {get_human_time(eta_time)}'
        print(status, end='')

    with session.get(url, timeout=120, stream=True) as response:
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

    print('')


def _download_files(work_dir: Path, mirror_url: str, branch: str, names: List[str]):
    for item in work_dir.iterdir():
        if item.name in names:
            item.unlink()
    total = len(names)
    with Session() as session:
        for idx, name in enumerate(names, start=1):
            _download(session, f'{mirror_url}/{branch}/os/x86_64/{name}', work_dir / name, idx, total)


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
            print(message, end='')
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
        print(message, end='')
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
    arg_parser.add_argument('-c', '--config', default=str(Path().home().joinpath('.config', 'armi.conf')), help='Config file.')
    arg_parser.add_argument('-d', '--destination-dir', default=str(Path().cwd().resolve()), help='Destination directory.')
    arg_parser.add_argument('-m', '--mirror-url', default=None, help='Mirror URL to download from.')
    arg_parser.add_argument('branches', nargs='*', default=BRANCHES, help='Branch(es) to download.')
    args = arg_parser.parse_args()

    config = Path(args.config)
    mirror_url = args.mirror_url or None
    if mirror_url:
        config.parent.mkdir(mode=0o755, exist_ok=True)
        config.write_text(f'{mirror_url}\n')
    if config.is_file():
        mirror_url = config.read_text().strip().removesuffix('/')
    else:
        print('ERROR: Mirror URL is required for update.')
        exit(1)

    for signo in (SIGINT, SIGTERM):
        signal(signo, lambda *unused_args: exit(1))

    errors = False
    for branch in args.branches:
        work_dir = Path(args.destination_dir, branch)
        work_dir.mkdir(mode=0o755, exist_ok=True)
        print(f'>>> Syncing branch {branch!r}.')
        _download_files(work_dir, mirror_url, branch, [f'{branch}.db.tar.gz', f'{branch}.files.tar.gz'])
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
            _download_files(work_dir, mirror_url, branch, [package.path.name for package in target_packages])
        else:
            errors = True
            print('>>> Previous attempt has been failed, trying one more time.')

    if errors:
        print('ERROR: Some branches have been failed to sync.')
        exit(1)
    else:
        Path(args.destination_dir, 'last_update').write_text(f'{ctime()}\n')


if __name__ == '__main__':
    main()
