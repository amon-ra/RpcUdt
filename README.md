RpcUdt
======

Simple python rpc implementation focused on file transfers. It uses UDT as backend protocol.

### Usage:

import multiprocessing,time
import rpcudt

def run_sum_server(ctx=None):
    class SumServer(object):
        def sum(self, x, y):
            return x + y

    server = rpcudt.RpcUdtServer(SumServer())
    server.bind("127.0.0.1:8000")
    server.run()

def call():
    client = rpcudt.RpcUdtClient()
    client.connect("127.0.0.1:8000")

    start = time.time()
    [client.sum(1, 2) for _ in xrange(NUM_CALLS)]

    print 'call: %d qps' % (NUM_CALLS / (time.time() - start))

multiprocessing.Process(target=run_sum_server)
call()

