#!/usr/bin/env python
#-*- coding:utf-8 -*-

# python sysbench_papapa.py --host=127.0.0.1 --user=hcloud --password=hcloud 

import os, sys
import click
import time
from collections import namedtuple
from utils import (
	logger,
    run_cmd,
    MySQL,
)

# user variables
class SB(object):
    sc = {
            'db': "sbtest",
            'table_size': 10000,
            'tables': 6,
            'run_time': 30,
            'threads': {
                        'normal': [4, 8]
                        },
            'oltp': {
                    'wr': "oltp_read_write.lua",
                    }
            }
    
    def __init__(self):
        self.oltp = self.sc['oltp'].get('wr')   
        self.oltp_nick = self.oltp
        self.db = self.sc['db']    
        self.table_size = self.sc['table_size']
        self.run_time = self.sc['run_time']
        self.tables = self.sc['tables']
        self.threads = self.sc['threads'].get('normal')

sb = SB()
###
# global variables
base_dir = os.path.split(os.path.realpath(__file__))[0]
script = os.path.basename(__file__)

logfile = "%s/%s.log" % (base_dir, script.split('.')[0])
logging = logger(logfile)

subprefix = time.strftime('%Y%m%d_%H%M%S',time.localtime(time.time()))
res_dir = "%s/sysbench_result/%s" % (base_dir, subprefix)

def _run(cmd, display=0):
    try:
        code, out, err = run_cmd(cmd, display)
    except Exception as err:
        logging.error("_run-%s exception, cmd: %s \nerr: %s." % (display, cmd, err))
        return 1, '', err 
    logging.info("_run-%s, cmd: %s \ncode: %s \nout: %s \nerr: %s." % (display, cmd, code, out, err))
    return int(code), out, err

def _message(msg, level='info'):
    if level == 'info':
        logging.info(msg)
    else:
        logging.error(msg)
    print msg

def init_mysql(host, user, password):
    try:
        h, p = host.split(':')[0], int(host.split(':')[1])
    except Exception as e:
        _message("MySQL host和port验证失败，例如127.0.0.1:3306，退出。")
        return False
    mysql_config = {
        'host': h,
        'port': p,
        'user': user,
        'password': password,
    }
    try:
        mysql = MySQL(mysql_config)
        return mysql
    except Exception as e:
        _message("MySQL服务登录异常: %s，退出。" % e)
        return False

def check_env(host, user, password):

    def _check_sysbench():
        cmd = "whereis sysbench"
        cs_code, cs_out, cs_err = _run(cmd)
        sb.bin = s_bin = cs_out.split(':')[-1].strip()
        if cs_code == 0 and s_bin:
            v_cmd = "%s --version" % s_bin
            v_code, v_out, v_err = _run(v_cmd)
            version = v_out.split()[-1]
            if v_code == 0 and version.startswith('1.0'):
                o_cmd = "find / -name %s" % sb.oltp
                o_code, o_out, o_err = _run(o_cmd)
                if o_code == 0 and o_out:
                    sb.oltp = o_out.split()[0]
                else:
                    _message("sysbench lua脚本未找到，退出。")
                    return False
                logging.info("sysbench检查通过。")
                return True
            else:
                _message("sysbench版本检查异常，需要大于1.0版本，退出。")
                return False
        else:
            _message("没有找到sysbench安装路径，确定安装了sysbench软件，退出。")
            return False

    def _check_mysql(_mysql, _sql):
        try:
            res = _mysql.execute(_sql)
            if res == 1:
                sb.prepare = 1
                logging.info("MySQL创建%s成功。" % (sb.db))
            elif res == 0:
                sb.prepare = 0
                logging.info("MySQL已经存在%s，未执行sql: %s。" % (sb.db, _sql))
            return True
        except Exception as e:
            _message("MySQL创建test db失败，sql: %s, error: %s, 退出。" % (_sql, e))
            return False

    mysql = init_mysql(host, user, password)
    sql = "CREATE DATABASE IF NOT EXISTS %s" % sb.db
    if _check_sysbench() and _check_mysql(mysql, sql):
        _message("环境检查通过，准备压测命令。")
        return True
    else:
        return False

def drop_testdb(host, user, password):

    def _drop_mysql(_mysql, _sql):
        try:
            res = _mysql.execute(_sql)
            if res == 1:
                _message("MySQL删除%s成功。" % (sb.db))
            elif res == 0:
                _message("MySQL中没有%s，未执行sql: %s。" % (sb.db, _sql))
            return True
        except Exception as e:
            _message("MySQL删除test db失败，sql: %s, error: %s, 退出。" % (_sql, e))
            return False

    sql = "DROP DATABASE IF EXISTS %s" % sb.db
    mysql = init_mysql(host, user, password)
    _drop_mysql(mysql, sql)

def sysbench_run(host, user, password):
    
    h, p = host.split(':')[0], int(host.split(':')[1])
    base_cmd = "{sb_bin} {sb_oltp}".format(sb_bin=sb.bin, sb_oltp=sb.oltp)
    parameter_opt = " --table_size={sb_ts} --db-driver=mysql --tables={sb_t}".format(sb_ts=sb.table_size, sb_t=sb.tables)
    auth_opt = " --mysql-host={host} --mysql-user={user} --mysql-port={port} --mysql-password={password}".format(host=h, 
                                                                                                                user=user,
                                                                                                                port=p,
                                                                                                                password=password
                                                                                                                )
    def _sb_run():
        if not os.path.isdir(res_dir):
            os.makedirs(res_dir)
        _message("将要开始压力测试，压测场景%s，并发%s，结果输出路径%s。" % (sb.oltp_nick.split('.')[0], sb.threads, res_dir))
        for t in sb.threads:
            _message("并发%s，run压测即将开始。" % t)
            run_opt = " --threads={threads} --time={time}".format(threads=t, time=sb.run_time)
            export_file = "%s/%s_%s.log" % (res_dir, sb.oltp_nick.split('.')[0], t)
            cmd = base_cmd + parameter_opt + run_opt + auth_opt + ' run > ' + export_file
            code, out, err = _run(cmd, sb.run_time)
            if code == 0:
                _message("并发%s，run压测已经完成。" % t)
            else:
                _message("run压测失败，请查看日志，退出。")
                return False
        return True        
        

    def _sb_prepare():
        cmd = base_cmd + parameter_opt + auth_opt + ' prepare'
        code, out, err = _run(cmd, 1)  
        if code == 0:
            _message("perpare测试数据完成。")
            return True
        else:
            _message("perpare测试数据失败，请查看日志，退出。")
            return False         
   
    if sb.prepare == 1:
        _message("准备perpare测试数据，%s * %s。" % (sb.tables, sb.table_size) )
        if not _sb_prepare():
            return False  
    elif sb.prepare == 0:
        _message("测试数据已经存在，不在执行prepare命令。")   
    if _sb_run():
        _message("sysbench执行压测全部完成。")
        return True
    else:
        return False
    

@click.command()
@click.option('--host', required=True, type=str, help='host_ip:mysql_port, ex: 127.0.0.1:3306.')
@click.option('--user', required=True, type=str, help='login MySQL user name.')
@click.option('--password', required=True, type=str, help='login MySQL password.')
@click.argument('drop', nargs=1, default=0)
def main(host, user, password, drop):
    if drop:
        drop_testdb(host, user, password)
        sys.exit(0)
    # check environment
    if not check_env(host, user, password):
        _message("环境检查失败，请查看日志，退出。")
        sys.exit(0)
    if not sysbench_run(host, user, password):
        _message("执行sysbench命令失败，请查看日志，退出。")
        sys.exit(0) 
    

if __name__ == "__main__":
    logging.info("========%s starting========" % script)
    main()
