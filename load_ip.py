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

def generate_statistics(files):
    """
    读取预处理之后的文件，将其处理成针对每个源的统计结果
    """
    data=[]
    stat_dict = {}
    files=sorted(files,reverse=True)
    files=sorted(files)
    print "source_iptimes_error:",files,"OK"
    new_ip=[]
    old_ip=[]
    for file_name in files:
        print file_name
        shell_code = 'grep -E "\[LOAD PROXY\].*proxies loaded" %s > /home/fangwang/statistic_scripts/routine_liuyu/temp_log/load_ip_log.txt' % file_name
        os.system(shell_code)
        with open('/home/fangwang/statistic_scripts/routine_liuyu/temp_log/load_ip_log.txt') as f:
            while True:
                each_content = f.readline()
                if each_content:
                    times=ip_pat.search(each_content).group(1)
                    if each_content.find("new_database")>-1:
                        times=times[0:times.find(" ")]
                        ip_list=new_ip_pat.findall(each_content)
                        port_list=new_port_pat.findall(each_content)
                        for index in range(0,len(ip_list)):
                            ip_list[index]=ip_list[index]+":"+port_list[index]
                        for index in range(0,int(times)):
                            new_ip.append(ip_list[index])
                    else:
                        ip_list=old_ip_pat.findall(each_content)
                        for index in range(0,int(times)):
                            old_ip.append(ip_list[index])
                else:
                    break
    data.append(new_ip)
    data.append(old_ip)
    return data

def stat_log():
    """
    log 统计的整个流程
    """
    insert_data=[]
    files = get_filename()
    data = generate_statistics(files)
    hour_time=files[0][23:31]+files[0][32:34]+"00"
    day_time=files[0][23:31]
    new_ip=data[0]
    old_ip=data[1]
    insert_data.append(len(old_ip))
    insert_data.append(len(set(old_ip)))
    insert_data.append(len(new_ip))
    insert_data.append(len(set(new_ip)))
    insert_data.append(len(new_ip)+len((old_ip)))
    insert_data.append(len(set(new_ip))+len(set(old_ip)))
    insert_data.append(hour_time)
    insert_data.append(day_time)

    print insert_data
    monitor.ExecuteSQL("insert into each_lib_ip(old_times,old_uniq_times,new_times,new_uniq_times,sum_times,sum_uniq_times,datetime,date) values (%s,%s,%s,%s,%s,%s,%s,%s)" , insert_data)
if __name__ == '__main__':
    stat_log()
    '''
    if len(sys.argv)==2:
        date=sys.argv[1]
        stat_log()
    else:
        print 'error!'
    '''
