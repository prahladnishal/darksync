import logging, traceback, sys

Log = logging
logging.basicConfig(level=logging.DEBUG)
def log_traceback(fault):
	msg = "Error %s:%s. Traceback -" % (str(fault.__class__), unicode(fault))
	msg += "".join(traceback.format_exception(*sys.exc_info()))
	Log.error(msg)

Log.traceback = log_traceback
__builtins__['Log'] = Log