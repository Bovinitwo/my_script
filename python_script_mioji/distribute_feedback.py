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

distribute_pat = re.compile(r'^(.*?),.*INFO (.*?) distribute status: (.*?):index.*,pos.*,(.*?),(.*?)$')
feedback_pat = re.compile(r'^(.*?),.*update_proxy\?source=(.*?)&proxy=(.*?)&error=(.*?)&speed')

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
    else:
        files.append(sys.argv[1])

    return files

def generate_statistics(files):
    """
    读取预处理之后的文件，将其处理成针对每个源的统计结果
    """
    
    stat_dict = {}
    files=sorted(files,reverse=True)
    #files=sorted(files)
    for file_name in files:
        print file_name
        shell_code = 'grep -E "INFO.*distribute status|update_proxy\?source=" %s > /home/fangwang/statistic_scripts/routine_liuyu/temp_log/distribute_feedback_log.txt' % file_name
        os.system(shell_code)
        with open('/home/fangwang/statistic_scripts/routine_liuyu/temp_log/distribute_feedback_log.txt') as f:
            content_list = f.readlines()
            for each_content in content_list:
                #print each_content
                try:
                    source=""
                    source_name=""
                    ip=""
                    content=[]
                    if each_content.find("distribute")<0:
                        feedback_content=feedback_pat.search(each_content).groups()
                        feedback=list(feedback_content)        
                        content.append(feedback[0])
                        content.append(feedback[3])
                        source_name=feedback_content[1]
                        ip=feedback_content[2]
                    else:
                        distribute_content = distribute_pat.search(each_content).groups()
                        distribute_content=list(distribute_content)
                        content.append(distribute_content[0])
                        content.append(distribute_content[3])
                        content.append(distribute_content[4])
                        source_name=distribute_content[1]
                        ip=distribute_content[2]

                    source=source_name
                    for typ in ['Car','Bus','MultiFlight','multiFlight','RoundFlight','roundFlight','Flight','Rail','ListHotel','listHotel','Hotel']:
                        if source_name.endswith(typ):
                            source=re.sub(typ+'$','',source_name)
                            break;

                    stat_dict.setdefault(ip,{})
                    stat_dict[ip].setdefault(source,[])
                    stat_dict[ip][source].append(content)
                except Exception, e:
                    continue
    return stat_dict
def stat_log():
    """
    log 统计的整个流程
    """

    files = get_filename()
    stat_dict = generate_statistics(files)

    old=sys.stdout
    f=open('result/distribute_feedback_result.txt','w')
    sys.stdout=f

    for ip,source_dict in stat_dict.items():
        print "proxy:",ip
        for source,content_list in source_dict.items():
            print "\tsource:",source
            for content in content_list:
                if len(content)==3:
                    print "\t\t",content[0],",","d:",content[1],",",content[2]
                if len(content)==2:
                    print "\t\t",content[0],",","f:",content[1]
    sys.stdout=old
    f.close()
if __name__ == '__main__':
    stat_log()
    '''
    if len(sys.argv)==2:
        date=sys.argv[1]
        stat_log()
    else:
        print 'error!'
    '''
