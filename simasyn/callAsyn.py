# -*- coding: utf-8 -*-
# Started by Alan
# MainTained by Alan
# Contact: alan@sinosims.com

# Archive call info from redis into MySQL
# run:
#   python callAsyn.py /path/to/pools/ production

import MySQLdb, ConfigParser, sys, redis, time, logging, traceback

env = sys.argv[2] if len(sys.argv) > 2 else 'test'

mcfg = ConfigParser.ConfigParser()
mcfg.read("%s/%s/mysql.ini"%(sys.argv[1], env))

conn = MySQLdb.connect(host=mcfg.get('mysql', 'host'), db=mcfg.get('mysql', 'db'), user=mcfg.get('mysql', 'usr'), passwd=mcfg.get('mysql', 'pwd'), charset="utf8")
conn.ping(True)
_cursor = conn.cursor()

rcfg = ConfigParser.ConfigParser()
rcfg.read("%s/%s/redis.ini"%(sys.argv[1], env))

pool = redis.ConnectionPool(host=rcfg.get('Srdb', 'host'), port=rcfg.getint('Srdb', 'port'), db=rcfg.getint('Srdb', 'db'))
_redis = redis.Redis(connection_pool=pool)

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
            hasEle = archive()
        except Exception, e:
            _logerr_(traceback.format_exc())
        if not hasEle: time.sleep(2)

    _cursor.close()
    conn.close()

def archive():
    nc = _redis.lpop('System:Calls')

    if not nc: return False

    ch = _redis.hgetall('Call:%s:info'%nc)

    if len(ch) > 8:
        n = 0
        try:
            sql = "insert into calling values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            param = (ch['id'], ch['cdid'], ch['cpid'], ch['bid'], ch['uid'], ch['oth'], int(ch['typ']), ch.get('rid', ''), int(ch['st']), int(ch['ed']))
            n = _cursor.execute(sql, param)
        except Exception, e:
            _logerr_(traceback.format_exc())
        if n == 1:
            _redis.zadd('User:%s:oth:%s:voices'%(ch['uid'], ch['oth']), ch['id'], int(ch['id'][-16:]))
            _redis.zadd('User:%s:oths'%ch['uid'], ch['id'][:-16:], int(ch['id'][-16:])) 
            _log_('SQ', "%s %s %s"%(ch['id'], ch['id'][-16:], ch['id'][:-16:]))
        else:
            esql = sql%param
            _log_('ER', "%d EXE SQL %s"%(n, esql))
    else:
        _log_('ER', "Call %s no info"%nc)
            
    conn.commit()
    return True

if __name__ == "__main__":
    main()
