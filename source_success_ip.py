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
                if time>datetime.datetime.now()-datetime.timedelta(days=2) and time<=datetime.datetime.now()-datetime.timedelta(days=1):
                    files.append(log_dir+'/'+each_file)
    return files

def generate_statistics(files):
    """
    读取预处理之后的文件，将其处理成针对每个源的统计结果
    """
    
    stat_dict = dict()
    files=sorted(files,reverse=True)
    #files=sorted(files)
    print "source_success_ip:",files,"OK"
    for file_name in files:
        #print file_name
        shell_code = 'grep "update_proxy?source" %s > /home/fangwang/statistic_scripts/routine_liuyu/temp_log/source_80_proxy_log' % file_name
        os.system(shell_code)
        with open('/home/fangwang/statistic_scripts/routine_liuyu/temp_log/source_80_proxy_log') as f:
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
                     
                    stat_dict.setdefault(source,{})
                    stat_dict[source].setdefault(proxy_ip,{})
                    
                    if error_code in stat_dict[source][proxy_ip]:
                        stat_dict[source][proxy_ip][error_code]+=1
                    else:
                        stat_dict[source][proxy_ip][error_code]=1
                except Exception, e:
                    continue
    return stat_dict

def stat_log():
    """
    log 统计的整个流程
    """

    files = get_filename()
    stat_dict = generate_statistics(files)
    m_log_time_hour=files[0][23:31]+files[0][32:34]+"00"
    m_log_time_day=files[0][23:31]
    print m_log_time_day
    #print m_log_time
    for source,proxy_ip_dict in stat_dict.items():
        insert_data=[]
        proxy_times=dict()
        proxy_80=[]
        sum_proxy=0
        error_0_times=0
        sum_times=0
        for proxy_ip,error_dict in proxy_ip_dict.items():
            proxy_ip_times=0
            for error,error_times in error_dict.items():
                proxy_ip_times+=stat_dict[source][proxy_ip][error]
                sum_times+=error_times
                if error=='0':
                    error_0_times+=error_times
            proxy_times.setdefault(proxy_ip,proxy_ip_times)
            sum_proxy+=proxy_ip_times
            if "0" in stat_dict[source][proxy_ip]:
                if stat_dict[source][proxy_ip]["0"]/float(proxy_ip_times)>=0.8:
                    proxy_80.append(proxy_ip)
        uniq_proxy_num=str(len(proxy_times))
        avg=str(sum_proxy/float(uniq_proxy_num))
        max_times=max(proxy_times.values())
        min_times=min(proxy_times.values())
        max_proxy=[]
        min_proxy=[]
        for proxy_ip,times in proxy_times.items():
            if max_times == times:
                max_proxy.append(proxy_ip)
            if min_times == times:
                min_proxy.append(proxy_ip)
        max_data=str(max_times)+"\n\n"+str(max_proxy)
        if len(min_proxy)<10:
            min_data=str(min_proxy)
        else:
            min_data=str(min_proxy[0:9])
        proxy_80_data=str(proxy_80)
        sum_proxy_str=str(sum_proxy)
        insert_data.append(source)
        insert_data.append(uniq_proxy_num)
        insert_data.append(sum_proxy_str)
        insert_data.append(avg)
        insert_data.append(max_times)
        insert_data.append(max_data)
        insert_data.append(min_times)
        insert_data.append(min_data)
        insert_data.append(len(proxy_80))
        insert_data.append(proxy_80_data)
        insert_data.append(str(len(proxy_80)/float(uniq_proxy_num)))
        insert_data.append(error_0_times)
        insert_data.append(sum_times)
        insert_data.append(error_0_times/float(sum_times))
        if sys.argv[1]=="-hour":
            insert_data.append(m_log_time_hour)
            res=monitor.ExecuteSQL("insert into each_source_success_ip_hour(source,num_proxy,req_times,avg_req,max_times,max_data,min_times,min_data,proxy_80_times,num_proxy_80_success,rate_success_80,error_0_times,sum_times,error_0_rate,datetime) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",insert_data)
        elif sys.argv[1]=="-day":
            insert_data.append(m_log_time_day)
            res=monitor.ExecuteSQL("insert into each_source_success_ip_day(source,num_proxy,req_times,avg_req,max_times,max_data,min_times,min_data,proxy_80_times,num_proxy_80_success,rate_success_80,error_0_times,sum_times,error_0_rate,date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",insert_data)
if __name__ == '__main__':
    stat_log()
