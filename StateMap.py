import os, sys, time, stat, copy
import logger

CHANGE_ADD = "ADD"
class StateMap:
    def __init__(self, path):
        self.basepath = path
        self.state = {}

    def create_state_map(self):
        if self.basepath[-1] <> '/':
            self.basepath = self.basepath + '/'
        Log.info('Walking %s', self.basepath)
        self.walk(self.basepath)

    def callback(self, fname, st):
        relfname = fname[len(self.basepath):]
        self.state[relfname] = (relfname, st.st_size)

    def walk(self, path):
        for f in os.listdir(path):
            pathname = os.path.join(path, f)
            st = os.stat(pathname)
            if stat.S_ISDIR(st[stat.ST_MODE]):
                self.walk(pathname)
            elif stat.S_ISREG(st[stat.ST_MODE]):
                self.callback(pathname, st)
    
    def get_change(self, ostate):
        for key, value in self.state.items():
            #If file is not present in other side
            if key not in ostate:
                #del self.state[]
                yield CHANGE_ADD, value[0]
                continue
            #If file is present but size is not same
            ovalue = ostate[key]
            if value[1] <> ovalue[1]:
                yield CHANGE_ADD, value[0]

    def get_state(self):
        return copy.deepcopy(self.state)


if __name__ == '__main__':
    f = sys.argv[1]
    s = StateMap(f)
    s.create_state_map()
    for c in s.get_change({}):
        print c
    print s.get_state()



