import os
import re
from flask import Flask, request, abort
from typing import *

app: Flask = Flask(__name__)

BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
DATA_DIR: str = os.path.join(BASE_DIR, "data")


def lim_it(it: Iterator, value: int) -> Iterator:
    i: int = 0
    for obj in it:
        if i < value:
            yield obj
        else:
            break
        i += 1


def need_cmd(it: Iterator, cmd: str, value: Any) -> Any:
    if cmd == 'filter':
        return filter(lambda x: value in x, it)
    if cmd == 'map':
        return map(lambda x: x.split(' ')[int(value)], it)
    if cmd == 'unique':
        return iter(set(it))
    if cmd == 'sort':
        return sorted(it, reverse=(value == 'desc'))
    if cmd == 'limit':
        return lim_it(it, int(value))
    if cmd == 'regex':
        regexp: re.Pattern[str] = re.compile(value)
        return filter(lambda v: regexp.findall(v), it)

    return it


def result_query(it: Iterator, cmd1: str, cmd2: str, value1: Union[str, int], value2: Union[str, int]) -> Iterator:
    it = need_cmd(it, cmd1, value1)
    it = need_cmd(it, cmd2, value2)
    return it


@app.route("/perform_query", methods=["POST"])
def perform_query() -> Any:

    req_json: Optional[Any] = request.json

    if req_json is None:
        abort(400, 'Bad request')

    cmd1: str = req_json.get("cmd1")
    val1: str = req_json.get("value1")
    cmd2: str = req_json.get("cmd2")
    val2: str = req_json.get("value2")
    filename: str = req_json.get("filename")

    if not all((cmd1, val1, filename)):
        abort(400, 'Wrong query')

    file_path: str = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return abort(400, 'Where is file?')

    with open(file_path, 'r') as file:
        result: Iterator[str] = result_query(file, cmd1, cmd2, val1, val2)
        content: str = '\n'.join(result) if result is not None else ''

    return app.response_class(content, content_type='text/plain')


if __name__ == '__main__':
    app.run(port=5000, debug=True)
