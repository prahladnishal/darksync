import socket, mimetools
import logging
import json
import threading
import sys
import traceback
import Queue
import xmlrpclib
import time
import logger
class HTTPSocket:
	def __init__(self, fd):
		self.fd = fd
		self.rfile = self.fd.makefile()
	
	def read_packet(self):
		reqtype, command, headers = self.read_headers()
		# print 'read_packet reqtype', reqtype
		# print 'read_packet command', command
		# print 'read_packet headers',  headers, headers.items()
		if reqtype == 'POST':
			ln = headers.get('Content-Length')
			body = self.recv_body(ln)
			#Log.info(('read_packet', command, headers.items(), repr(body)))
			return command, headers, body
		if reqtype == 'GET':
			body = {}
			#Log.info(('read_packet', command, headers.items(), repr(body)))
			return command, headers, body
		if reqtype.find('HTTP') > -1:
			ln = int(headers.get('Content-Length'))
			body = self.recv_body(ln)
			#Log.info(('read_packet', 'RESPONSE', headers.items(), repr(body)))
			return 'RESPONSE', headers, body

	def read_headers(self):
		retry = 16
		while True:
			first_line  = self.rfile.readline() 
			if not first_line and retry == 0:
				raise Exception('EOF')
			if not first_line:
				retry -= 1
				continue
			break
		first_line = first_line.split()
		if len(first_line) <>  3:
			raise Exception('Invalid line %s' % first_line)
		reqtype, command, http_version = first_line
		headers = mimetools.Message(self.rfile, 0)

		return reqtype, command, headers

	def recv_body(self, size):
		size = int(size)
		data = self.rfile.read(size)
		return data

	def sendall(self, data):
		#Log.info(('sendall', data))
		self.fd.sendall(data)

	def make_headers(self, headers):
		hdrs = []
		for key, value in headers.items():
			hdrs.append(str(key) + ':' + str(value))
		return hdrs

	def send_request_packet(self, method, headers, body):
		url = '/api/%s' % (method.replace('.', '/'))
		hdrs = ["POST %s HTTP/1.0" % url]
		hdrs.extend(self.make_headers(headers))
		hdrs.append("Content-Length:%s" % len(body))
		hdrs = "\r\n".join(hdrs)
		hdrs += '\r\n\r\n'
		self.sendall(hdrs+body)
		#self.sendall(body)

	def send_response_packet(self, headers, body):
		hdrs = ['HTTP/1.0 200 OK']
		hdrs.append("Content-Length: %s" % (len(body),))
		hdrs.extend(self.make_headers(headers))
		hdrs = "\r\n".join(hdrs)
		hdrs += '\r\n\r\n'
		self.sendall(hdrs)
		self.sendall(body)

	def shutdown(self):
		try:
			self.fd.shutdown(socket.SHUT_RDWR)
			self.fd.close()
		except:
			pass
		self.fd = None


class Handler(threading.Thread):
	def __init__(self, sock, parent):
		threading.Thread.__init__(self)
		self.parent = parent
		self.sock = sock
		self.abort = False
		self._read_lock = threading.Lock()
		self._write_lock = threading.Lock()
		self.workers = []
		self.ctx = {'fp' : {} }

	def run(self):
		try:
			self.httpsock = HTTPSocket(self.sock)
			for i in range(8):
				th = threading.Thread(target=self.handler, args=(self.httpsock, ))
				self.workers.append(th)
				th.start()
			for worker in self.workers:
				worker.join()
			self.httpsock.shutdown()
			if self.parent.cbdisconnect:
				self.parent.cbdisconnect(self.ctx)
			

		except Exception, fault:
			Log.error('Error in Handler.run %s', str(fault))

	def handler(self, httpsock):
		threading.currentThread().localdata = self.ctx
		while True:
			try:
				if self.abort:
					break
				with self._read_lock:
					if self.abort:
						break
					command, headers, body = httpsock.read_packet()
				try:
					methodname = command[5:].replace('/', '.')
					seqid = headers.get('seqid')
					if body:
						params = json.loads(body)
					else:
						params = []
					response = self.parent.callmethod(methodname, params)
					rc = 0
				except Exception,  fault:
					rc = 1
					response = "Error: %s" % str(fault)
					Log.traceback(fault)
				response = json.dumps([rc, response])
				with self._write_lock:
					httpsock.send_response_packet({'seqid': seqid}, response)
			except Exception, fault:
				self.abort = True
				Log.error('Error in handler: %s', str(fault))
				Log.traceback(fault)

class HTTPServer:
	def __init__(self, host, port, cbdisconnect=None):
		self.host = host
		self.port = port
		self.methods = {}
		self.Handler = Handler
		self.cbdisconnect = cbdisconnect

	def callmethod(self, methodname, params):
		if methodname not in self.methods:
			Log.error('Invalid method %s.', methodname)
			return "No method named %s" % methodname
		try:
			t1 = time.time()
			return self.methods[methodname](*params)
		finally:
			Log.info('Time taken in %s: %s ms', methodname, int(time.time() - t1) )


	def register_method(self, methodname, callback):
		self.methods[methodname] = callback

	def start(self):
		s = socket.socket()
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.bind((self.host, self.port))
		Log.info('Server started on %s:%s', self.host, self.port)
		s.listen(5)
		while True:
			csock, caddr = s.accept()
			print csock, caddr
			handler = self.Handler(csock, self)
			handler.start()
	

class HTTPClient:
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.fd = None
		self.queue = Queue.Queue()
		self.seqdict = {}
		self.rseqid = 1
		self.seqid = 1
		self.__lock = threading.Lock()
		self._write_lock = threading.Lock()
		self.connect()
		self.abort = False
		self.fault = None

	def connect(self):
		s = socket.socket()
		s.connect((self.host, self.port))
		self.fd = s
		self.httpsock = HTTPSocket(s)
		self.reader_thread = threading.Thread(target=self.reader)
		self.reader_thread.start()

	def reader(self):
		try:
			print 'reader thread started'
			while True:
				if self.abort:
					raise Exception('Aborted')
				reqtype, headers, body = self.httpsock.read_packet()
				#Log.info(('Client read_packet', reqtype, headers, body))
				seqid = int(headers.get('seqid'))
				if not seqid:
					seqid = self.rseqid
					self.rseqid = self.rseqid + 1
				event = self.seqdict[seqid]['event']
				body = json.loads(body, encoding='utf-8')
				self.seqdict[seqid]['response'] = body
				event.set()
		except Exception, fault:
			if self.abort:
				return
			Log.error('Stopping reader thread for %s %s: %s' %(self.host, self.port, str(fault)))
			Log.traceback(fault)
			self.fault = str(fault)

	def __safe_request(self, methodname, params):
		#Log.info(('__safe_request', methodname, params))
		with self._write_lock:
			if self.fault:
				raise Exception(self.fault)
			seqid = self.seqid 
			self.seqid = self.seqid + 1
			event = threading.Event()
			self.seqdict[seqid] = {'event' : event, 'response' : None}
			params = json.dumps(params, encoding='utf-8')
			self.httpsock.send_request_packet(methodname, {'seqid' : seqid}, params)
		while True:
			if self.abort:
				raise Exception('Aborted')
			if self.fault:
				raise Exception(self.fault)
			if event.isSet():
				rc, response  = self.seqdict[seqid]['response']
				if rc:
					raise Exception(response)
				return response
			event.wait(10)

	def __getattr__(self, name):
		return xmlrpclib._Method(self.__safe_request,  name)

	def abort(self):
		self.abort = True
		self.shutdown()

	def shutdown(self):
		if not self.fd:
			return
		self.fd.shutdown(socket.SHUT_RDWR)
		self.fd.close()
		self.fd = None
		self.abort = True

	def __str__(self):
		return "HTTPClient %s:%s" % (self.host, self.port)

def v1_test_method1(x=1, y=1):
	mydata = threading.currentThread().localdata
	ctx = mydata
	print 'v1_test_method1', ctx, x, y
	ctx['test'] = 1
	time.sleep(5)
	return {'method': 'v1_test_method1', 'x' : x,'y': y}


def test_client_method(th, srv, params):
	print 'in thread', th, srv.v1.test_method1(*params)

if __name__ == '__main__':
	if sys.argv[1] == 'server':
		httpserver = HTTPServer('127.0.0.1', 8080)
		httpserver.register_method('v1.test_method1', v1_test_method1)
		httpserver.start()
	elif sys.argv[1] == 'client':
		httpclient = HTTPClient('127.0.0.1', 8080)
		rc = httpclient.v1.test_method1('hellox', 'helloy')
		print 'rc', rc
		rc1 = httpclient.v1.test_method1('1', '2')
		print 'rc1', rc1
		httpclient.shutdown()
	elif sys.argv[1] == 'threadedclient':
		httpclient = HTTPClient('127.0.0.1', 8080)
		for i in range(16):
			threading.Thread(target=test_client_method, args=(i, httpclient, (i, i))).start()
		time.sleep(20)
		httpclient.shutdown()
	elif sys.argv[1] == 'httpclient':
		import httplib
		conn = httplib.HTTPConnection('127.0.0.1:8080')
		conn.debuglevel = 9
		conn.request("POST", "/api/v1/test_method1", "test=test", {})
		response = conn.getresponse()
		print response.status, response.reason
		print response.read()


