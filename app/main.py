import os

from flask import Flask, request, abort

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def lim_it(it, value):
    i = 0
    for obj in it:
        if i < value:
            yield obj
        else:
            break
        i += 1


def need_cmd(it, cmd, value):
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

    return it


def result_query(it, cmd1, cmd2, value1, value2):
    it = need_cmd(it, cmd1, value1)
    it = need_cmd(it, cmd2, value2)
    return it


@app.route("/perform_query", methods=["POST"])
def perform_query():
    req_json = request.json
    cmd1 = req_json["cmd1"]
    val1 = req_json["value1"]
    cmd2 = req_json["cmd2"]
    val2 = req_json["value2"]
    filename = req_json["filename"]

    if not all(cmd1 and val1 and filename):
        abort(400, 'Wrong query')

    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return abort(400, 'Where is file?')

    with open(file_path, 'r') as file:
        result = result_query(file, cmd1, cmd2, val1, val2)
        content = '\n'.join(result) if result is not None else ''

    return app.response_class(content, content_type='text/plain')


if __name__ == '__main__':
    app.run(port=5000, debug=True)
