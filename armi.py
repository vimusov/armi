#!/usr/bin/python

from argparse import ArgumentParser
from contextlib import closing
from hashlib import md5
from operator import attrgetter
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from subprocess import check_call
from tarfile import TarFile
from tempfile import NamedTemporaryFile
from time import ctime
from typing import Iterator, List, NamedTuple

Package = NamedTuple('Package', [('path', Path), ('size', int), ('checksum', str)])
ATTEMPTS = 2
BUF_SIZE = 4 * 2**20
BRANCHES = ('core', 'extra', 'community', 'multilib')
URI_TEMPLATE = '/{branch:s}/os/x86_64/{name:s}'


def _download_files(work_dir: Path, mirror_url: str, branch: str, names: List[str]):
    for item in work_dir.iterdir():
        if item.name in names:
            item.unlink()
    with NamedTemporaryFile(mode='wt') as tmp_file:
        for name in names:
            tmp_file.write(mirror_url + URI_TEMPLATE.format(branch=branch, name=name) + '\n')
        tmp_file.flush()
        check_call(['wget', '-nv', '--show-progress', '-c', '-P', str(work_dir), '-i', tmp_file.name])


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

    # http://mir.archlinux.fr/
    config = Path(args.config)
    mirror_url = args.mirror_url.rstrip('/') if args.mirror_url else None
    if mirror_url:
        config.parent.mkdir(mode=0o755, exist_ok=True)
        config.write_text(f'{mirror_url}\n')
    if config.is_file():
        mirror_url = config.read_text().strip()
    else:
        print('ERROR: Mirror URL is required for update.')
        exit(1)

    for signo in (SIGINT, SIGTERM):
        signal(signo, lambda *unused: exit(1))

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
