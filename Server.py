import threading
import os
import DarkHTTPServer
import logger
import sys
import base64
import StateMap
import json

SHARE_IDENTIFIER = {}

def get_path(shareid, fname=''):
	foldername = SHARE_IDENTIFIER[shareid]
	n = os.path.join(foldername, fname)
	return n

class FileObject:
	def __init__(self, fname, size):
		self.fname = fname
		self.size = size
		tfname = fname
		if os.path.exists(fname):
			uname = self.get_unique_fname(fname)
			os.rename(fname, uname)
			Log.info('File already exists %s, renaming to %s', fname, uname)
		dirname = os.path.dirname(tfname)
		try:
			os.makedirs(dirname)
		except:
			pass
		self.fd = open(tfname, 'w+')
		self.__lock = threading.Lock()

	def get_unique_fname(self, fname):
		count = 1
		while True:
			tname = fname + '.tmp.' + str(count)
			if os.path.exists(tname):
				count += 1
				continue
			return tname

	def write(self, offset, size, data):
		assert size == len(data)
		data = base64.b64decode(data)
		with self.__lock:
			self.fd.seek(offset)
			self.fd.write(data)
	
	def commit(self):
		self.fd.flush()
		os.fsync(self.fd.fileno())
		self.fd.close()

	def close(self):
		self.fd.close()

def v1_upload_file(shareid, fname, size):
	ctx = threading.currentThread().localdata
	fname = get_path(shareid, fname)
	Log.info('Start file %s', fname)
	ctx['fp'][fname] = FileObject(fname, size)

def v1_upload_file_data(shareid, fname, offset, size, data):
	ctx = threading.currentThread().localdata
	fname = get_path(shareid, fname)
	fobj = ctx['fp'][fname]
	fobj.write(offset, size, data)

def v1_commit_file(shareid, fname):
	ctx = threading.currentThread().localdata
	fname = get_path(shareid, fname)
	fobj = ctx['fp'][fname]
	fobj.commit()
	Log.info('Commit file %s', fname)
	del ctx['fp'][fname]

def v1_get_state_map(shareid):
	foldername = get_path(shareid)
	s = StateMap.StateMap(foldername)
	s.create_state_map()
	return s.get_state()

def register_methods(httpserver):
	httpserver.register_method('v1_upload_file', v1_upload_file)
	httpserver.register_method('v1_upload_file_data', v1_upload_file_data)
	httpserver.register_method('v1_commit_file', v1_commit_file)
	httpserver.register_method('v1_get_state_map', v1_get_state_map)

def cbdisconnect(ctx):
	for k, v in ctx['fp'].items():
		Log.info('Closing file %s', k)
		v.close()

def parse_config(fname):
	global SHARE_IDENTIFIER
	data = open(fname).read().strip()
	print 'read config', repr(data)
	d = json.loads(data)
	SHARE_IDENTIFIER = d
	print type(d), d

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print "Usage: python Server config_fname port"
	else:
		config_fname = sys.argv[1]
		parse_config(config_fname)
		port = int(sys.argv[2])
		httpserver = DarkHTTPServer.HTTPServer('0.0.0.0', port)
		register_methods(httpserver)
		httpserver.start()
