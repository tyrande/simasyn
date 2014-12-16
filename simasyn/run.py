# -*- coding: utf-8 -*-
# Started by Alan
# MainTained by Alan
# Contact: alan@sinosims.com

# import MySQLdb, apnsclient, ConfigParser, sys, redis, time, logging, traceback, base64
import MySQLdb, apns, ConfigParser, sys, redis, time, logging, traceback, base64


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
        # self._srv = apnsclient.APNs(apnsclient.Session().get_connection("push_sandbox", cert_file="%s/%s/ca/simhub.pem"%(poo, env)))
        self._certFile = "%s/%s/ca/simhub.pem"%(poo, env)
        self._srv = apns.APNs(use_sandbox=True, cert_file=self._certFile)
        self._connStart = time.time()
        super(RingAsyn, self).__init__(poo, env)

    def run(self):
        ring = self._redis.lpop('System:Ring')
        if not ring: return False
        _log_('PS', ring)
        now = time.time()
        if now - self._connStart > 300:
            self._srv = apns.APNs(use_sandbox=True, cert_file=self._certFile)
            self._connStart = now 
        toks = ring.split(',')
        # msg = apnsclient.Message(toks[0:-2], alert=u"%s \u6765\u7535..."%toks[-2], badge=1)
        # self._srv.send(msg)
        payload = apns.Payload(alert=u"%s \u6765\u7535..."%toks[-2], sound="income_ring.caf", badge=1)
        [ self._srv.gateway_server.send_notification(t, payload) for t in set(toks[0:-2]) ]
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
        _log_('PO', nc)

        ch = self._redis.hgetall('Call:%s:info'%nc)
        uid = ch.get('uid', None)
        if len(ch) > 8:
            n = 0
            try:
                cmid = ch['inum'] + ch['id'][-16:]
                sql = "insert into calling (id, cdid, cpid, bid, uid, oth, fnum, inum, loc, seq, typ, rid, st, ed) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                param = (cmid, ch['cdid'], ch['cpid'], ch['bid'], uid, ch['oth'], ch['fnum'], ch['inum'], ch['loc'], ch['id'], int(ch['typ']), ch.get('rid', ''), int(ch['st']), int(ch['ed']))
                n = self._cursor.execute(sql, param)
            except Exception, e:
                _logerr_(traceback.format_exc())
            if n == 1:
                if uid and uid != '':
                    _script = """
                        redis.call('zadd', 'User:'..KEYS[1]..':oths', KEYS[3], KEYS[2])
                        redis.call('zadd', 'User:'..KEYS[1]..':oth:'..KEYS[2]..':voices', KEYS[3], KEYS[2]..KEYS[3])
                    """
                    self._redis.eval(_script, 3, uid, ch['inum'], ch['id'][-16:])
                else:
                    _script = """
                        local ids = redis.call('zrange', 'Box:'..KEYS[4]..':Users', 0, -1)
                        for k, v in pairs(ids) do
                            redis.call('zadd', 'User:'..v..':oths', KEYS[3], KEYS[2])
                            redis.call('zadd', 'User:'..v..':oth:'..KEYS[2]..':voices', KEYS[3], KEYS[2]..KEYS[3])
                        end
                    """
                    self._redis.eval(_script, 4, uid, ch['inum'], ch['id'][-16:], ch['bid']) 
            else:
                _log_('ER', "%d %s"%(n, repr(param)))
        else:
            _log_('ER', "Call %s no info"%nc)
                
        self._conn.commit()
        return True    

class SmsingAsyn(Asyn):
    def __init__(self, poo, env):
        # self._srv = apnsclient.APNs(apnsclient.Session().get_connection("push_sandbox", cert_file="%s/%s/ca/simhub.pem"%(poo, env)))
        self._srv = apns.APNs(use_sandbox=True, cert_file="%s/%s/ca/simhub.pem"%(poo, env))
        super(SmsingAsyn, self).__init__(poo, env)

    def run(self):
        sms = self._redis.lpop('System:Smsing')
        if not sms: return False
        _log_('PS', sms)
        toks = sms.split(',')
        s = self._redis.hgetall('Sms:%s:info'%toks[-1])
        # msg = apnsclient.Message(toks[0:-1], alert="%s %s..."%(s['oth'], base64.b64decode(s['msg'])), badge=1)
        # self._srv.send(msg)
        payload = apns.Payload(alert="%s %s..."%(s['oth'], base64.b64decode(s['msg'])), sound="default", badge=1)
        [ self._srv.gateway_server.send_notification(t, payload) for t in set(toks[0:-1]) ]
        return True

class SmsAsyn(Asyn):
    def __init__(self, poo, env):
        mcfg = ConfigParser.ConfigParser()
        mcfg.read("%s/%s/mysql.ini"%(poo, env))

        self._conn = MySQLdb.connect(host=mcfg.get('mysql', 'host'), db=mcfg.get('mysql', 'db'), user=mcfg.get('mysql', 'usr'), passwd=mcfg.get('mysql', 'pwd'), charset="utf8")
        self._conn.ping(True)
        self._cursor = self._conn.cursor()
        super(SmsAsyn, self).__init__(poo, env)

    def run(self):
        nc = self._redis.lpop('System:Sms')

        if not nc: return False
        _log_('PO', nc)
        sh = self._redis.hgetall('Sms:%s:info'%nc)
        uid = sh.get('uid', None)
        if len(sh) > 7:
            n = 0
            try:
                sql = "insert into sms (id, cdid, cpid, bid, uid, oth, fnum, inum, loc, msg, st, ed) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                param = (sh['id'], sh['cdid'], sh['cpid'], sh['bid'], uid, sh['oth'], sh['fnum'], sh['inum'], sh['loc'], sh['msg'], int(sh['st']), int(sh['ed']))
                n = self._cursor.execute(sql, param)
            except Exception, e:
                raise e
                _logerr_(traceback.format_exc())
                return False
            if n == 1:
                if uid:
                    _script = """
                        redis.call('zadd', 'User:'..KEYS[1]..':othsms', KEYS[3], KEYS[2])
                        redis.call('zadd', 'User:'..KEYS[1]..':othsms:'..KEYS[2]..':sms', KEYS[4], KEYS[2]..KEYS[3])
                    """
                    self._redis.eval(_script, 4, uid, sh['id'][:-16:], sh['id'][-16:], sh['st'])
                else:
                    _script = """
                        local ids = redis.call('zrange', 'Box:'..KEYS[4]..':Users', 0, -1)
                        for k, v in pairs(ids) do
                            redis.call('zadd', 'User:'..v..':othsms', KEYS[3], KEYS[2])
                            redis.call('zadd', 'User:'..v..':othsms:'..KEYS[2]..':sms', KEYS[5], KEYS[2]..KEYS[3])
                        end
                    """
                    self._redis.eval(_script, 5, uid, sh['id'][:-16:], sh['id'][-16:], sh['bid'], sh['ed'])
            else:
                _log_('ER', "%d %s"%(n, repr(param)))
        else:
            _log_('ER', "SMS %s no info"%nc)

        self._conn.commit()
        return True 

def main():
    env = sys.argv[3] if len(sys.argv) > 3 else 'test'

    if len(sys.argv) < 3: return help()
    if sys.argv[1] == 'ring':
        asyn = RingAsyn(sys.argv[2], env)
    elif sys.argv[1] == 'call':
        asyn = CallAsyn(sys.argv[2], env)
    elif sys.argv[1] == 'smsing':
        asyn = SmsingAsyn(sys.argv[2], env)
    elif sys.argv[1] == 'sms':
        asyn = SmsAsyn(sys.argv[2], env)
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
