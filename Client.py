import os
import sys
import Queue
import logging
import traceback
import DarkHTTPServer
import logger
import base64
import time
import threading

import StateMap
READ_SZ = 1024 * 1024 * 16

class WorkQ:
	def __init__(self):
		self.threads = []
		self.queue = Queue.Queue()
		self.faulted = False

	def start(self, wcount=8):
		for i in range(4):
			th = threading.Thread(target=self.handler)
			self.threads.append(th)
			th.start()

	def put(self, func, args=()):
		self.queue.put((func, args))
		Log.info('Size of queue:%s', self.queue.qsize())

	def handler(self):
		while True:
			try:
				item = self.queue.get()
				func, args = item
				if func is None:
					break
				#if self.faulted:
				#	break
				func(*args)
			except Exception, fault:
				Log.error(fault)
				#self.faulted = True
			finally:
				self.queue.task_done()
				Log.info('Size of queue:%s', self.queue.qsize())

	def join(self):
		for th in self.threads:
			self.put(None, None)
		for th in self.threads:
			th.join()

class SyncFile:
	def __init__(self, client, fname, basepath, wqueue=None):
		self.fname = fname
		self.basepath = basepath
		self.client = client
		self.wqueue = wqueue
		self.fault = None

	def sync(self):
		try:
			absname = os.path.join(self.basepath, self.fname)
			Log.info('Syncing file %s, %s', self.fname, absname)
			self.fd = open(absname, 'r+')
			statinfo = os.stat(absname)
			size = statinfo.st_size
			self.client.v1_start_file(self.fname, size)

			done = 0
			while done < size:
				if self.fault:
					raise Exception(self.fault)
				data = self.fd.read(READ_SZ)
				self.sync_data(done, data)
				done += len(data)
			self.commit()
		except Exception, fault:
			Log.error('Failed sync of file %s: %s', self.fname, str(fault))
			Log.traceback(fault)
			raise

	def sync_data(self, offset, data):
		try:
			data = base64.b64encode(data)
			self.client.v1_sync_file_data(self.fname, offset, len(data), data)
		except Exception, fault:
			raise

	def commit(self):
		self.client.v1_commit_file(self.fname)
		self.fd.close()

class SyncFolder:
	def __init__(self, path, httpclient):
		self.path = path
		self.httpclient = httpclient

	def sync(self):
		try:
			self.dosync()
		except Exception, fault:
			Log.traceback(fault)
			raise

	def dosync(self):
		state_map = StateMap.StateMap(self.path)
		other_state_map = self.httpclient.v1_get_state_map(self.path)
		state_map.create_state_map()
		gen = state_map.get_change(other_state_map)
		self.workerQ = WorkQ()
		self.workerQ.start()
		for change_type, fname in gen:
			s = SyncFile(self.httpclient, fname, self.path)
			self.workerQ.put(s.sync)
		self.workerQ.join()


if __name__ == '__main__':
	foldername = sys.argv[1]
	httpclient = DarkHTTPServer.HTTPClient('192.168.0.101', 8080)
	try:
		t1 = time.time()
		syncfile = SyncFolder(foldername, httpclient)
		syncfile.sync()
	finally:
		httpclient.shutdown()
	print 'total time taken', time.time() - t1
