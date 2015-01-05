#!/usr/bin/env python
import os, sys, struct, multiprocessing
import time
import logging
import collections
import hmac,zlib

import udt
import gevent
import cbor


PACKETSIZE=34

def make_digest(message,key=None):
    "Return a digest for the message."
    if key:
        return hmac.new(key, message, hashlib.sha1).digest()
    else:
        return zlib.crc32(message)

class RpcServerUdt(object):

    def __init__(self, obj, threads=10, max_bandwidth=1024*52*10):
        self._methods = self._parse_methods(obj)
        #SOCK_DGRAM: El mensaje se emite como un bloque    
        self.socket = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.thread_count = 0
        self.threads_num = threads
        self.clients = None
        self.epoll = None
        self.max_bandwidth =max_bandwidth


    @staticmethod
    def _parse_methods(self, obj):
        methods = {}
        for method in dir(obj):
            if not method.startswith('_'):
                methods[method] = getattr(obj, method)
        # for method in dir(self):
        #     if not method.startswith('_'):
        #         methods[method] = getattr(self, method)

        return methods

    @staticmethod
    def _extract_name(methods):
        return getattr(type(methods), '__name__', None) or repr(methods)

    def _format_args_spec(self, args_spec, r=None):
        if args_spec:
            r = [dict(name=name) for name in args_spec[0]]
            default_values = args_spec[3]
            if default_values is not None:
                for arg, def_val in zip(reversed(r), reversed(default_values)):
                    arg['default'] = def_val
        return r

    def _rpc_inspect(self):
        methods = dict((m, f) for m, f in self._methods.items()
                    if not m.startswith('_'))
        detailled_methods = dict((m,
            dict(args=self._format_args_spec(f._zerorpc_args()),
                doc=f._zerorpc_doc())) for (m, f) in methods.items())
        return {'name': self._name,
                'methods': detailled_methods}

    def _inject_builtins(self):
        self._methods['_zerorpc_list'] = lambda: [m for m in self._methods
                if not m.startswith('_')]
        self._methods['_zerorpc_name'] = lambda: self._name
        self._methods['_zerorpc_ping'] = lambda: ['pong', self._name]
        self._methods['_zerorpc_help'] = lambda m: \
            self._methods[m]._zerorpc_doc()
        self._methods['_zerorpc_args'] = \
            lambda m: self._methods[m]._zerorpc_args()
        self._methods['_zerorpc_inspect'] = self._zerorpc_inspect


    def __call__(self, method, *args,**kwargs):
        if method not in self._methods:
            raise NameError(method)
        return self._methods[method](*args)

    def _print_traceback(self, protocol_v1, exc_infos):
        logger.exception('')

        exc_type, exc_value, exc_traceback = exc_infos
        if protocol_v1:
            return (repr(exc_value),)
        human_traceback = traceback.format_exc()
        name = exc_type.__name__
        human_msg = str(exc_value)
        return (name, human_msg, human_traceback)

    def bind(self, *args):
        #self.socket.bind(*args)
        try:
            self.socket.setsockopt(0, udt.UDT_REUSEADDR, True)
            self.socket.setsockopt(0, udt.UDT_SNDSYN, False)
#           self.udtserversock.setsockopt(0, udt.UDT_RCVSYN, False)
            self.socket.bind(*args)
            self.socket.listen(10)
        except socket.error:
            print 'UDT server socket bind error'            
        self.thread_count = 0
        self.clients = {}
        self.epoll = udt.epoll()
       

    def _send(self, msg, req, pid, sec, enc=0):
        #calcular el crc
        size = len(msg)
        #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
        header=cbor.dumps(struct.pack("!IIHIII",pid,sec,self.version,enc,size,crc))
        self.clients[req].send(header,0) 
        self.clients[req].send(msg,0) 

    def _stream(self,req,pid,sec,gen_pid,packetsize,size,enc=0):
        try:
            msg=self.gens[gen_pid].next()
            size=len(msg)
        except:
            #Last packet
            size=0
        h2=cbor.dumps([size,size,"RAW",gen_pid])
        self._send(h2,req, pid, sec ,enc)
        if size > 0:
            self.clients[req].send(msg) 

    #close socket if size is 0
    def _istop(self,h2size=0):
        if h2size = 0:
            self.clients[i].send("OK", 0)
            self.clients[i].close()
            self.epoll.remove_usock(i)
            del self.clients[i]
            return True
        return False

    def _getData(self,req,size,bufsize=8096):
        i=bufsize
        while i <= size:
            yield self.clients[req].recv(bufsize,0)
            i+=bufsize
        rest = size % bufsize
        if rest > 0: 
            yield self.clients[req].recv(rest,0)

    def _route (self):
        return cbor.dumps([0,0,"ROUTE","NEW ip:port"])

    def _response(self,method,args):
        if method = "STREAM":
            self._stream(req,pid,sec,packetsize,size)
            return True
        return False

    def _process2(self,req,h2size,crcr,enc=False):
        try:
            method,packetsize,size,args = cbor.loads(self.clients[req].recv(h2size,0))
        except:
            #must send the error
            return
        #test crc
        #generate an iterator for the data
        if self._response():
            return
        if packetsize > 0: 
            data = self._getData(req,size,packetsize)
        args.append(data)
        try:
            rtn = self.methods[method](*args)
        except:
            #si se provoca una route exception devolvemos ROUTE
            rtn=self._route()
        if isinstance(rtn, collections.Iterable) and (not isinstance(rtn, collections.Sized)):
            #is an generator
            gen_pid=urandom(24)
            while gen_pid in self.gens:
                gen_pid=urandom(24)
            self.gens[gen_pid]=rtn
            rtn=cbor.dumps(["STREAM",0,0,gen_pid])
        else:
            rtn=cbor.dumps(["RET",0,0,rtn])

        self._send(rtn,req,pid,sec)
        #Send the data ruturns

    #Recivimos los parametros del packete de cabecera: Tamaño de buffer. Tamaño de la cabezera del paquete
    #El formato de la cabezera sera: tamaño(1),sha(20),crc(8)
    #en esta primera version no se utiliza un buffer sino que se lee el fich completo
    #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
    #H2 (puede ir encriptada) :  methodo, tamaño de buffer, tamaño de data , argumentos 
    #El crc es calculado uniendo H1 y H2
    def _process(self, req):
        # header, method, args = req
        # rtn = self.methods[method](*args)
        # h = {'response_to': 0, 'message_id':0, 'v': 3}
        # return (h, 'OK', [rtn])
        try:
            pid,sec,version,enc,h2size,crcr = cbor.loads(self.clients[req].recv(PACKETSIZE,0))
            #Finalizamos si el tamaño es excesivo (para evitar ataques)
            if h2size > MAX_PACKETSIZE:
                h2size =0
            if self._istop(h2size):
                return None
        except:
            return self._istop()
        return gevent.spawn(self._process2,req,h2size,crcr,enc)
        # text = self.clients[i].recv(1024, 0)
        # print text

        # if text != "OK":
        #     gevent.spawn(handle,self.clients[i])
        #     #p = Thread(target=test, args=(clients[i], text))
        #     #p.start()
        #     poll.append(p)
        # else:
        #     #print text
        #     self.clients[i].send("OK", 0)
        #     self.clients[i].close()
        #     self.epoll.remove_usock(i)
        #     del clients[i]

    def _accept(self):
        while len(self.clients) <  self.threads_num:
            child, host = self.socket.accept()
            child.setsockopt(0, udt.UDT_MAXBW, self.max_bandwidth)
            self.clients[child.fileno()] = child
            self.epoll.add_usock(child.fileno(), udt.UDT_EPOLL_IN) 

    def run(self):
        # while True:
        #     req = self._recv()
        #     rep = self._process(req)
        #     self._send(rep)
        # for x in xrange(0, self.threads_num):
        #     #accept crea un nuevo cliente
        #     child, host = self.socket.accept()
        #     child.setsockopt(0, udt.UDT_MAXBW, self.max_bandwidth)
        #     self.clients[child.fileno()] = child
        #     self.epoll.add_usock(child.fileno(), udt.UDT_EPOLL_IN)
        gevent.spawn(_accept)         
        poll=[]
        while True:
            print 'wait..'
            sets = self.epoll.epoll_wait(-1)
            #print sets
            poll = []
            for i in sets[0]:
                p= self._process(i)
                if p:
                    poll.append(p)
            if not clients:
                self.epoll.release()
                break
        try:
            gevent.joinall(poll)
        except:
            pass

    def close (self):
        self.socket.close()

    def __getattr__(self, method):
        return lambda *args, **kwargs: self(method, *args, **kwargs)


class RpcClientUdt(object):

    def __init__(self,enc=0,version=0,packetsize=8192):
        self.socket = udt.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.slist = {}
        self.gen = None
        self.pid = 0
        self.sec = 0
        self.enc = enc
        self.version = version
        self.packetsize = packetsize

    def _select_pattern(self, event):
        for pattern in patterns.patterns_list:
            if pattern.accept_answer(event):
                return pattern
        msg = 'Unable to find a pattern for: {0}'.format(event)
        raise RuntimeError(msg)

    def _process_response(self, request_event, bufchan, timeout):
        #reply_event = bufchan.recv(timeout)
        pattern = self._select_pattern(reply_event)
        return pattern.process_answer(self._context, bufchan, request_event,
                    reply_event, self._handle_remote_error)


    def __getattr__(self, method):
        return lambda *args, **kwargs: self(method, *args, **kwargs)

    def __call__(self, method, *args, **kwargs):
        self._send(method, args)
        h, status, rtn = self._recv()
        return rtn[0]


    def _sendmore(self,socket,size,msg,method,args):
        #packetsize,size,method,args = self._sockrecv(socket)
        if size = 0:
            try:
                while True:
                    data = msg.next()
                    self._socksend()
                    socket.send(,0)
            except StopIteration:
                pass
        else:
            i=self.packetsize
            while i <= size:
                socket.send(msg,self.packetsize)
                i+=self.packetsize
            rest = size % self.packetsize
            if rest > 0: 
                socket.send(msg,self.packetsize)
            
    def _socksend(self,sec=0,msg=None):
        if msg:
            crc = make_digest(msg) & 0xffffffff
            #calcular el crc
            crc=0
        size = len(msg)
        #H1 Cabecera (No encriptada): Identificacion del client,secuencia,Encriptacion (ninguna,cabeceras,cabeceras + datos, tamaño h2, tamaño total,crc)
        header=cbor.dumps(struct.pack("!IIHIII",self.pid,sec,self.version,self.enc,size,crc))
        socket.send(header,0) 
        socket.send(h2,0) 

    def _send(self, method,args, msg=None):
        size=0
        psize=0
        sec = 0
        try:
            socket=self.slist[method]
        except:
            socket=self.socket        
        if isinstance(msg, collections.Iterable) and (not isinstance(msg,collections.Sized)):
            size = self.packetsize
            sec = 1
        elif msg:
            size = len(msg)
            sec = 1
            #psize = self.packetsize
        h2=cbor.dumps([psize,size,method,args])

        if sec>0:
            self._sendmore(socket,size,msg,method,args)
        else:
            self._socksend(method,args,sec,size)

    def _sockrecv(self,socket):
        try:
            pid,sec,version,enc,h2size,crcr = cbor.loads(socket.recv(PACKETSIZE,0))
            #Finalizamos si el tamaño es excesivo (para evitar ataques)
        except:
            return self._istop()            
        if h2size > MAX_PACKETSIZE:
            h2size =0
        if self._istop(h2size):
            return None
        data = socket.recv(h2size,0)
        crc = make_digest(data) & 0xffffffff
        if crc != crcr:
            raise error 
        if version > 1:
            return cbor.loads(data)
        else:
            return data

    def _recv(self,method=None):
        try:
            socket=self.slist[method]
        except:
            socket=self.socket
        return self._sockrecv(socket)

    def connect(self, *args):
        self.socket.connect(*args)


