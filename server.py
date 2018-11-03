import socket
import uuid
import pickle
from time import time
from re import compile

def run():
    class Server:
        def __init__(self, ip, port):
            self._ip = ip
            self._port = port
            self._cmd_patterns = self.patterns()

        def patterns(self):
            cmd_patterns = ['(?P<cmd_type>ADD) (?P<queue_name>\S+) (?P<data_len>\d+) (?P<data>.+)',
                            '(?P<cmd_type>ACK) (?P<queue_name>\S+) (?P<task_id>.+)',
                            '(?P<cmd_type>GET) (?P<queue_name>\S+)',
                            '(?P<cmd_type>IN) (?P<queue_name>\S+) (?P<task_id>.+)',
                            '(?P<cmd_type>SAVE)']
            for i in range(len(cmd_patterns)):
                cmd_patterns[i] = compile(bytes(cmd_patterns[i], 'utf8'), re.DOTALL)
            return cmd_patterns

        def start(self, callback):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self._ip, self._port))
            sock.listen(1)
            while True:
                conn, addr = sock.accept()
                data = conn.recv(1024)
                if not data:
                    break
                cmd = self._parse_cmd(data)
                if cmd:
                    data_len = cmd.get('data_len')
                    if data_len:
                        data_len = int(data_len.decode())
                        cmd['data'] += self._recieve(conn, data_len - len(cmd['data']))
                    conn.send(ans)
                else:
                    conn.send(b"ERROR")
                conn.close()

        def _parse_cmd(self, data):
            for cmd_pattern in self._cmd_patterns:
                cmd = cmd_pattern.match(data)
                if cmd:
                    return cmd.groupdict()

        def _recieve(self, conn, data_len):
            data = bytes()
            while len(data) < data_len:
                piece = conn.recv(data_len - len(data))
                if piece == '':
                    raise RuntimeError()
                data += piece
            return data

    class Queue:
        def __init__(self, timeout):
            self._timeout = timeout
            self._queue = {}

        def add(self, queue_name, data, data_len, id):
            if queue_name in self._queue:
                self._queue[queue_name].update({id: {"data": data, "data_len": data_len, "timestart": 0}})
            else:
                self._queue.update({queue_name: {id: {"data": data, "data_len": data_len, "timestart": 0}}})
            return id

        def get(self, queue_name):
            queue = self._queue.get(queue_name)
            now_time = time()
            if queue:
                for i in queue:
                    if now_time - int(queue[i]["timestart"]) >= self._timeout:
                        queue[i]["timestart"] = now_time
                        result = queue[i].copy()
                        result.update({"task_id": i})
                        result.pop("timestart")
                        return result

        def ack(self, queue_name, queue_id):
            if self.in_queue(queue_name, queue_id):
                return self._queue.get(queue_name).pop(queue_id)

        def in_queue(self, queue_name, queue_id):
            return self._queue.get(queue_name).get(queue_id)

    class Work():
        def __init__(self, ip, port, timeout):
            self._path = './'
            self._server = Server(ip, int(port))
            self._queue = Queue(int(timeout))

        def run_(self):
            self.restore()
            self.restore_from_journal()
            self._server.start(self._cmd)

        def save(self):
            with open(self._path+'queue.txt', 'wb') as f:
                pickle.dump(self._queue, f)
                self.clean_journal()
            return True

        def restore(self):
            f = open(self._path + 'queue.txt', 'rb')
            self._queue = pickle.load(f)
            f.close()

        def restore_from_journal(self):
            try:
                f = open(self._path + 'journal.txt', 'rb')
                i = 0
                while True:
                    try:
                        cmd = pickle.load(f)
                        res = self._cmd(cmd, False)
                        i += 1
                    except EOFError:
                        break
            except FileNotFoundError:
                print('FileNotFoundError')
            else:
                f.close()

        def write_journal(self, cmd):
            with open(self._path+'journal.txt', 'ab') as f:
                pickle.dump(cmd, f)

        def clean_journal(self):
            with open(self._path + 'journal.txt', 'w') as f:
                print('')

        def _cmd(self, cmd, journal=True):
            if cmd['cmd_type'] == b'ADD':
                return self._add(cmd, journal)

            if cmd['cmd_type'] == b'GET':
                return self._get(cmd)

            if cmd['cmd_type'] == b'ACK':
                return self._ack(cmd, journal)

            if cmd['cmd_type'] == b'IN':
                return self._in(cmd)

            if cmd['cmd_type'] == b'SAVE':
                return self._save()

        def _add(self, cmd, journal):
            if not cmd.get('task_id'):
                cmd.update({"task_id": uuid.uuid4().bytes})
            if journal:
                self.write_journal(cmd)
            return self._queue.add(cmd['queue_name'], cmd['data'], cmd['data_len'], cmd['task_id'])

        def _run_get(self, cmd):
            result = self._queue.get(cmd['queue_name'])
            if result:
                result = result['task_id'] + b" " + result['data_len'] + b" " + result['data']
                return result
            return b'NONE'

        def _ack(self, cmd, journal):
            if journal:
                self.write_journal(cmd)
            if self._queue.ack(cmd['queue_name'], cmd['task_id']):
                return b'YES'
            else:
                return b'NO'

        def _in(self, cmd):
            if self._queue.in_queue(cmd['queue_name'], cmd['task_id']):
                return b'YES'
            else:
                return b'NO'

        def _save(self):
            if self.save():
                return b'OK'

    serv = Work(ip = 5555, port = '0.0.0.0', timeout = 300)
    serv.run_()

if __name__ == '__main__':
    run()
