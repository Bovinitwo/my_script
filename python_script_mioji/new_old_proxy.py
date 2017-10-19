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
            if re.match(r'proxy.*_\d\d\.log',each_file) and os.path.isfile(log_dir+'/'+each_file):
                time = datetime.datetime.strptime(each_file[5:16],'%Y%m%d_%H')
                if time>datetime.datetime.now()-datetime.timedelta(hours=4) and time<=datetime.datetime.now()-datetime.timedelta(hours=1):
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
    data=[]
    stat_dict = {}
    files=sorted(files,reverse=True)
    files=sorted(files)
    print "source_iptimes_error:",files,"OK"
    new_uniq_ip=[]
    old_uniq_ip=[]
    new_times=0
    old_times=0
    for file_name in files:
        print file_name
        shell_code = 'grep -E "\[LOAD PROXY\].*proxies loaded|update_proxy\?source=" %s > /home/fangwang/statistic_scripts/routine_liuyu/temp_log/load_ip_log.txt' % file_name
        os.system(shell_code)
        with open('/home/fangwang/statistic_scripts/routine_liuyu/temp_log/load_ip_log.txt') as f:
            while True:
                each_content = f.readline()
                if each_content:
                    if each_content.find("LOAD PROXY")>-1:
                        times = ip_pat.search(each_content).group(1)
                        #print "time",times
                        if each_content.find("new_database")>-1:
                            times=times[0:times.find(" ")]
                            new_times+=int(times)
                            ip_list=new_ip_pat.findall(each_content)
                            port_list=new_port_pat.findall(each_content)
                            for index in range(0,len(ip_list)):
                                ip_list[index]=ip_list[index]+":"+port_list[index]
                            for index in range(0,int(times)):
                                new_uniq_ip.append(ip_list[index])
                        else:
                            old_times+=int(times)
                            ip_list=old_ip_pat.findall(each_content)
                            for each_ip in ip_list:
                                old_uniq_ip.append(each_ip)
                    else:
                        log_content = log_pat.search(each_content).groups()
                        #print "log_content",log_content
                        source_name, proxy_string, error_code, speed = log_content 
                        if '.' in proxy_string and ':' in proxy_string:
                            proxy_ip = proxy_string
                        else:
                            continue
                        stat_dict.setdefault(proxy_ip,[0,0])
                        stat_dict[proxy_ip][1]+=1
                        if error_code == '0':
                            stat_dict[proxy_ip][0]+=1
                else:
                    break
    new_db_set=set(new_uniq_ip)
    old_db_set=set(old_uniq_ip)
    repeat_db_set=set(new_db_set & old_db_set)
    new_db_set=new_db_set-repeat_db_set
    old_db_set=old_db_set-repeat_db_set
    data.append(new_db_set)
    data.append(old_db_set)
    data.append(repeat_db_set)
    data.append(stat_dict)
    return data

def stat_log():
    """
    log 统计的整个流程
    """

    files = get_filename()
    data = generate_statistics(files)
    hour_time=files[0][23:31]+files[0][32:34]+"00"
    day_time=files[0][23:31]

    new_db_set=data[0]
    old_db_set=data[1]
    repeat_db_set=data[2]
    stat_dict=data[3]
    new_db_able_num=0
    old_db_able_num=0
    repeat_db_able_num=0
    
    new_db_success_times=0
    old_db_success_times=0
    repeat_db_success_times=0

    new_db_sum_success=0
    old_db_sum_success=0
    repeat_db_sum_success=0
    for ip,ip_success in stat_dict.items():
        if ip in new_db_set:
            if ip_success[0]>0:
                new_db_able_num+=1
            new_db_success_times+=ip_success[0]
            new_db_sum_success+=ip_success[1]
        if ip in old_db_set:
            if ip_success[0]>0:
                old_db_able_num+=1
            old_db_success_times+=ip_success[0]
            old_db_sum_success+=ip_success[1]
        if ip in repeat_db_set:
            if ip_success[0]>0:
                repeat_db_able_num+=1
            repeat_db_success_times+=ip_success[0]
            repeat_db_sum_success+=ip_success[1]

    new_db_success_rate=0
    old_db_success_rate=0
    repeat_db_success_rate=0

    if new_db_sum_success!=0:
        new_db_success_rate=new_db_success_times/float(new_db_sum_success)
    if old_db_sum_success!=0:
        old_db_success_rate=old_db_success_times/float(old_db_sum_success)
    if repeat_db_sum_success!=0:
        repeat_db_success_rate=repeat_db_success_times/float(repeat_db_sum_success)
    
    new_db_able_rate=0
    old_db_able_rate=0
    repeat_db_able_rate=0
   
    if new_db_able_num!=0:
        new_db_able_rate=new_db_able_num/float(len(new_db_set))
    if old_db_able_num!=0:
        old_db_able_rate=old_db_able_num/float(len(old_db_set))
    if repeat_db_able_num!=0:
        repeat_db_able_rate=repeat_db_able_num/float(len(repeat_db_set))
    
   
    print "新库的代理总次数成功率",new_db_success_rate,"(",new_db_sum_success,")"
    print "老库的代理总次数成功率",old_db_success_rate,"(",old_db_sum_success,")"
    print "重复的代理总次数成功率",repeat_db_success_rate,"(",repeat_db_sum_success,")"
    print "新库的可用代理比例",new_db_able_rate,"(",len(new_db_set),")"
    print "老库的可用代理比例",old_db_able_rate,"(",len(old_db_set),")"
    print "重复的可用代理比例",repeat_db_able_rate,"(",len(repeat_db_set),")"
    
    print "总成功率",(new_db_success_times+old_db_success_times+repeat_db_success_times)/float((new_db_sum_success+old_db_sum_success+repeat_db_sum_success))

    #monitor.ExecuteSQL("insert into each_lib_ip(old_times,old_uniq_times,new_times,new_uniq_times,sum_times,sum_uniq_times,datetime,date) values (%s,%s,%s,%s,%s,%s,%s,%s)" , insert_data)
if __name__ == '__main__':
    stat_log()
    '''
    if len(sys.argv)==2:
        date=sys.argv[1]
        stat_log()
    else:
        print 'error!'
    '''
