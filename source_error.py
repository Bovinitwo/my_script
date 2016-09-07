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
    print "source_iptimes_error:",files,"OK"
    for file_name in files:
        #print file_name
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
                     
                    stat_dict.setdefault(source,{})
                    if error_code=="0" or error_code=="22" or error_code=="23" or error_code=="24":
                        if error_code in stat_dict[source]:
                            stat_dict[source][error_code]+=1
                        else:
                            stat_dict[source][error_code]=1
                    else:
                        if "other" in stat_dict[source]:
                            stat_dict[source]["other"]+=1
                        else:
                            stat_dict[source]["other"]=1
                except Exception, e:
                    continue
    
    return stat_dict

def stat_log():
    """
    log 统计的整个流程
    """

    files = get_filename()
    stat_dict = generate_statistics(files)
    stat_dict_rate=dict()
    m_log_time=files[0][23:31]+files[0][32:34]+"00"
    #print m_log_time
    insert_data=[]
    for source,error_dict in stat_dict.items():
        for error,error_times in error_dict.items():
            insert_data=[]
            source_times=0
            for error,error_times in error_dict.items():
                source_times+=stat_dict[source][error]
            insert_data.append(source)
            insert_data.append(source_times)
            if "0" in stat_dict[source]:
                insert_data.append(str(stat_dict[source]["0"]))
                insert_data.append(str(stat_dict[source]["0"]/float(source_times)))
            else:
                insert_data.append("0")
                insert_data.append("0")
            if "22" in stat_dict[source]:
                insert_data.append(str(stat_dict[source]["22"]))
                insert_data.append(str(stat_dict[source]["22"]/float(source_times)))
            else:
                insert_data.append("0")
                insert_data.append("0")
            if "23" in stat_dict[source]:
                insert_data.append(str(stat_dict[source]["23"]))
                insert_data.append(str(stat_dict[source]["23"]/float(source_times)))
            else:
                insert_data.append("0")
                insert_data.append("0")
            if "24" in stat_dict[source]:
                insert_data.append(str(stat_dict[source]["24"]))
                insert_data.append(str(stat_dict[source]["24"]/float(source_times)))
            else:
                insert_data.append("0")
                insert_data.append("0")
            if "other" in stat_dict[source]:
                insert_data.append(str(stat_dict[source]["other"]))
                insert_data.append(str(stat_dict[source]["other"]/float(source_times)))
            else:
                insert_data.append("0")
                insert_data.append("0")
            insert_data.append(m_log_time)
            monitor.ExecuteSQL("insert into each_source_error_hour(source,source_times,error_0_times,error_0_rate,error_22_times,error_22_rate,error_23_times,error_23_rate,error_24_times,error_24_rate,error_other_times,error_other_rate,datetime) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" , insert_data)
if __name__ == '__main__':
    stat_log()
    '''
    if len(sys.argv)==2:
        date=sys.argv[1]
        stat_log()
    else:
        print 'error!'
    '''
