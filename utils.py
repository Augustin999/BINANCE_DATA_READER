from pathlib import Path
import os


def data_path():
    path = Path(os.getcwd())
    while path.name != 'Coding Projects':
        path = path.parent
    path = path / 'data'
    return path


def create_data_path():
    path = Path(os.getcwd())
    while path.name != 'Coding Projects':
        path = path.parent
    path = path / 'data'
    os.mkdir(path)
    return


def file_path(pair: str, timeframe: str):
    path = Path(os.getcwd())
    while path.name != 'Coding Projects':
        path = path.parent
    path = path / 'data'
    path = path / pair.upper() / f"{pair.upper()}{timeframe}.csv"
    return path


def read_last_line(path: Path):
    with open(path, "rb") as file:
        file.seek(-2, 2)
        while file.read(1) != b'\n':
            file.seek(-2, 1)
        last_line = file.readline().decode()
    return last_line


def append_lines(file_path: Path, rows: list):
    f = open(file_path, 'a')
    for row in rows:
        _line = ""
        for v in row:
            _line += str(v) + ','
        _line = _line[:-1] + '\n'
        f.write(_line)
    f.close()
    return
