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
            if re.match(r'proxy.*_\d\d\.log',each_file) and os.path.isfile(log_dir+'/'+each_file):
                time = datetime.datetime.strptime(each_file[5:16],'%Y%m%d_%H')
                if time>datetime.datetime.now()-datetime.timedelta(hours=2) and time<=datetime.datetime.now()-datetime.timedelta(hours=1):
                    files.append(log_dir+'/'+each_file)
    if sys.argv[1]=="-day":
        for each_file in all_files:
            if re.match(r'proxy.*_\d\d\.log',each_file) and os.path.isfile(log_dir+'/'+each_file):
                time = datetime.datetime.strptime(each_file[5:13],'%Y%m%d')
                if time>datetime.datetime.now()-datetime.timedelta(days=12) and time<=datetime.datetime.now()-datetime.timedelta(days=1):
                    files.append(log_dir+'/'+each_file)
    return files

def generate_statistics(files):
    """
    读取预处理之后的文件，将其处理成针对每个源的统计结果
    """
    
    stat_dict = dict()
    files=sorted(files,reverse=True)
    #files=sorted(files)
    print "source_iptimes_error:",files,"OK"
    for file_name in files:
        print file_name
        shell_code = 'grep "update_proxy?source" %s > /home/fangwang/statistic_scripts/routine_liuyu/temp_log/source_iptimes_error_log.txt' % file_name
        os.system(shell_code)
        with open('/home/fangwang/statistic_scripts/routine_liuyu/temp_log/source_iptimes_error_log.txt') as f:
            content_list = f.readlines()
            for each_content in content_list[::-1]:
                #print each_content
                try:
                    log_content = log_pat.search(each_content).groups()
                    source_name, proxy_string, error_code, speed = log_content 
                    if '.' in proxy_string and ':' in proxy_string:
                        proxy_ip = proxy_string
                    else:
                        continue

                    source=source_name 
                    for typ in ['Car','Bus','MultiFlight','multiFlight','RoundFlight','roundFlight','Flight','Rail','ListHotel','listHotel','Hotel']:
                        if source_name.endswith(typ):
                            source=re.sub(typ+'$','',source_name)
                            break
                     
                    stat_dict.setdefault(proxy_ip,0)
                    if error_code=='0':
                        stat_dict[proxy_ip]=1

                except Exception, e:
                    continue
    
    return stat_dict

def stat_log():
    """
    统计可用的代理个数，有一次error_code=0及为成功
    """

    files = get_filename()
    stat_dict = generate_statistics(files)
    ip_sum=len(stat_dict)
    ip_able=0
    for proxy_ip,able in stat_dict.items():
        if able==1:
            ip_able+=1
    print ip_sum
    print ip_able/float(ip_sum)
if __name__ == '__main__':
    stat_log()
