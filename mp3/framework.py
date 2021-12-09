import asyncio
import re
from collections import defaultdict
import json
import sys
from logging import INFO, ERROR, WARN, DEBUG

class Process:
    def __init__(self, pid, subproc):
        self.pid = pid
        self.subproc = subproc
        self.message_queue = asyncio.Queue()
        self.writer_task = asyncio.create_task(self.writer())

    @classmethod
    async def create(cls, pid, command, *args, **kwargs):
        subproc = await asyncio.create_subprocess_exec(command, *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)

        self = cls(pid, subproc, **kwargs)
        return self

    def send_message(self, msg):
        self.message_queue.put_nowait(msg)
    
    # write to stdin
    async def writer(self):
        while True:
            msg = await self.message_queue.get()
            res = bytes(msg, 'utf-8')
            self.subproc.stdin.write(res)
            await self.subproc.stdin.drain()

async def main():
    # await alog.init()
    f = open('in.txt', 'r')
    names = ['abc', 'def']
    clients = []
    tasks = []
    for pid in range(int(sys.argv[1])): # python3.10 framework.py 5 ./raft
        # python3.10 framework.py 2 client.go config.txt
        name = names[pid]
        p = await Process.create(str(pid), sys.argv[2], name+" "+sys.argv[3], sys.argv[1])
        
        clients.append(p)

    for line in f.readlines():
        temp = line.split(' ', 1)
        clients[int(temp[0])].send_message(temp[1])

    for pid in range(int(sys.argv[1])):
        tasks += [clients[pid].writer_task]

    await asyncio.gather(*tasks)
    
if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())