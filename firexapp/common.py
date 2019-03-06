import os
import re
import shlex
import socket
import subprocess

def delimit2list(str_to_split, delimiters=(',', ';', '|', ' ')) -> []:
    if not str_to_split:
        return []

    if not isinstance(str_to_split, str):
        return str_to_split

    # regex for only comma is (([^,'"]|"(?:\\.|[^"])*"|'(?:\\.|[^'])*')+)
    regex = """(([^""" + "".join(delimiters).replace(" ", "\s") + """'"]|"(?:\\.|[^"])*"|'(?:\\.|[^'])*')+)"""
    tokens = re.findall(regex, str_to_split)

    # unquote "tokens" if necessary
    tokens = [g1 if g1.strip() != g2.strip() or g2[0] not in "'\"" else g2.strip(g2[0]) for g1, g2 in tokens]
    tokens = [t.strip() for t in tokens]  # remove extra whitespaces
    tokens = [t.strip("".join(delimiters) + " ") for t in tokens]  # remove any extra (or lone) delimiters
    tokens = [t for t in tokens if t]  # remove empty tokens
    return tokens


def reserve_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    return port


def silent_mkdir(path, mode=0o777, exist_ok=True):
    os.makedirs(path, mode=mode, exist_ok=exist_ok)