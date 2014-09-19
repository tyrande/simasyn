# -*- coding: utf-8 -*-
# Started by Alan
# MainTained by Alan
# Contact: alan@sinosims.com

# Asynchronous push notice to user through Apple Push Notification service
# run:
#   python asynRing.py /path/to/pools/ production

import apnsclient, ConfigParser, sys, redis, time, logging, traceback

env = sys.argv[2] if len(sys.argv) > 2 else 'test'

rcfg = ConfigParser.ConfigParser()
rcfg.read("%s/%s/redis.ini"%(sys.argv[1], env))

pool = redis.ConnectionPool(host=rcfg.get('Srdb', 'host'), port=rcfg.getint('Srdb', 'port'), db=rcfg.getint('Srdb', 'db'))
_redis = redis.Redis(connection_pool=pool)

con = apnsclient.Session().get_connection("push_sandbox", cert_file="%s/%s/ca/simhub.pem"%(sys.argv[1], env))
srv = apnsclient.APNs(con)

def _log_(lv, obj):
    sys.stdout.write("[%s] %s %s\n"%(time.strftime("%Y-%m-%d %H:%M:%S"), lv, obj))
    sys.stdout.flush()

def _logerr_(err):
    tm = time.strftime("%Y-%m-%d %H:%M:%S")
    for s in err.split('\n'):
        sys.stderr.write('[%s] %s\n'%(tm, s))
    sys.stderr.flush() 

def main():
    hasEle = False
    while True:
        try:
            hasEle = pushToAPNs()
        except Exception, e:
            _logerr_(traceback.format_exc())
        if not hasEle: time.sleep(1)
    _cursor.close()
    conn.close()

def pushToAPNs():
    ring = _redis.lpop('System:Ring')
    if not ring: return False
    toks = ring.split(',')
    msg = apnsclient.Message(toks[0:-2], alert=u"%s \u6765\u7535..."%toks[-2], badge=1)
    srv.send(msg)
    _log_('PS', ring)
    return True

if __name__ == "__main__":
    main()
