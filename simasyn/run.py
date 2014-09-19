# -*- coding: utf-8 -*-
# Started by Alan
# MainTained by Alan
# Contact: alan@sinosims.com

import MySQLdb, apnsclient, ConfigParser, sys, redis, time, logging, traceback

def _log_(lv, obj):
    sys.stdout.write("[%s] %s %s\n"%(time.strftime("%Y-%m-%d %H:%M:%S"), lv, obj))
    sys.stdout.flush()

def _logerr_(err):
    tm = time.strftime("%Y-%m-%d %H:%M:%S")
    for s in err.split('\n'):
        sys.stderr.write('[%s] %s\n'%(tm, s))
    sys.stderr.flush() 

class Asyn(object):
    def __init__(self, poo, env):
        rcfg = ConfigParser.ConfigParser()
        rcfg.read("%s/%s/redis.ini"%(poo, env))
        _pool = redis.ConnectionPool(host=rcfg.get('Srdb', 'host'), port=rcfg.getint('Srdb', 'port'), db=rcfg.getint('Srdb', 'db'))
        self._redis = redis.Redis(connection_pool=_pool)


class RingAsyn(Asyn):
    def __init__(self, poo, env):
        self._srv = apnsclient.APNs(apnsclient.Session().get_connection("push_sandbox", cert_file="%s/%s/ca/simhub.pem"%(poo, env)))
        super(RingAsyn, self).__init__(poo, env)

    def run(self):
        ring = self._redis.lpop('System:Ring')
        if not ring: return False
        toks = ring.split(',')
        msg = apnsclient.Message(toks[0:-2], alert=u"%s \u6765\u7535..."%toks[-2], badge=1)
        self._srv.send(msg)
        _log_('PS', ring)
        return True

class CallAsyn(Asyn):
    def __init__(self, poo, env):
        mcfg = ConfigParser.ConfigParser()
        mcfg.read("%s/%s/mysql.ini"%(poo, env))

        self._conn = MySQLdb.connect(host=mcfg.get('mysql', 'host'), db=mcfg.get('mysql', 'db'), user=mcfg.get('mysql', 'usr'), passwd=mcfg.get('mysql', 'pwd'), charset="utf8")
        self._conn.ping(True)
        self._cursor = self._conn.cursor()
        super(CallAsyn, self).__init__(poo, env)

    def run(self):
        nc = self._redis.lpop('System:Calls')

        if not nc: return False

        ch = self._redis.hgetall('Call:%s:info'%nc)

        if len(ch) > 8:
            n = 0
            try:
                sql = "insert into calling values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                param = (ch['id'], ch['cdid'], ch['cpid'], ch['bid'], ch['uid'], ch['oth'], int(ch['typ']), ch.get('rid', ''), int(ch['st']), int(ch['ed']))
                n = self._cursor.execute(sql, param)
            except Exception, e:
                _logerr_(traceback.format_exc())
            if n == 1:
                self._redis.zadd('User:%s:oth:%s:voices'%(ch['uid'], ch['oth']), ch['id'], int(ch['id'][-16:]))
                self._redis.zadd('User:%s:oths'%ch['uid'], ch['id'][:-16:], int(ch['id'][-16:])) 
                _log_('SQ', "%s %s %s"%(ch['id'], ch['id'][-16:], ch['id'][:-16:]))
            else:
                esql = sql%param
                _log_('ER', "%d EXE SQL %s"%(n, esql))
        else:
            _log_('ER', "Call %s no info"%nc)
                
        self._conn.commit()
        return True    


def main():
    env = sys.argv[3] if len(sys.argv) > 3 else 'test'

    if len(sys.argv) < 3: return help()
    if sys.argv[1] == 'ring':
        asyn = RingAsyn(sys.argv[2], env)
    elif sys.argv[1] == 'call':
        asyn = CallAsyn(sys.argv[2], env)
    else:
        return help()

    hasEle = False
    while True:
        try:
            hasEle = asyn.run()
        except Exception, e:
            _logerr_(traceback.format_exc())
        if not hasEle: time.sleep(1)
    _cursor.close()
    conn.close()

def help():
    print 'simasyn command /path/to/pools env'

if __name__ == "__main__":
    main()
