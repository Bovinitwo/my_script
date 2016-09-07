# -*- coding: utf-8 -*-
import sys
sys.path.append('/home/workspace/ProxyServer/bin')
sys.path.append('/home/fangwang/statistic_scripts/')
import os
import time
import datetime
#import db_local as db
from DBHandle import DBHandle
import re
import json
#from send_mail import send_mail

monitor = DBHandle("10.10.86.250","root","miaoji@2014!","monitor")

ip_pat = re.compile(r'\[LOAD PROXY\] (.*?) proxies loaded ')
new_ip_pat=re.compile(r'\'ip\': u\'(.*?)\'')
old_ip_pat=re.compile(r'\'ip\': u\'(.*?)\'')
new_port_pat=re.compile(r'\'port\': (.*?)L')

log_pat = re.compile(r'update_proxy\?source=(.*?)&proxy=(.*?)&error=(.*?)&speed=(.*?) ')
log_dir = '/search/log/proxy'

def get_filename():
    """
    获取当前时间应该统计的 log 文件名
    """
    all_files = os.listdir(log_dir)
    files=[]
    if sys.argv[1]=="-hour":
        for each_file in all_files:
            if re.match(r'proxy.*_\d\d.log',each_file) and os.path.isfile(log_dir+'/'+each_file):
                time = datetime.datetime.strptime(each_file[5:16],'%Y%m%d_%H')
                if time>datetime.datetime.now()-datetime.timedelta(hours=2) and time<=datetime.datetime.now()-datetime.timedelta(hours=1):
                    files.append(log_dir+'/'+each_file)
    if sys.argv[1]=="-day":
        for each_file in all_files:
            if re.match(r'proxy.*_\d\d\.log',each_file) and os.path.isfile(log_dir+'/'+each_file):
                time = datetime.datetime.strptime(each_file[5:13],'%Y%m%d')
                if time>datetime.datetime.now()-datetime.timedelta(days=3) and time<=datetime.datetime.now()-datetime.timedelta(days=1):
                    files.append(log_dir+'/'+each_file)
    return files

