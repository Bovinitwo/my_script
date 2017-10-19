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

monitor = DBHandle("10.10.86.250","root","miaoji@2014!","workload")
if __name__ == '__main__':
    stat_dict={}
    times=0
    res=monitor.QueryBySQL("select * from workload_moniter where day>=20160811")
    for line in res:
        source_flag={}
        source_error=json.loads(line['errors'])
        #times+=1
        for source,error in source_error.items():
            for typ in ['Car','Bus','MultiFlight','multiFlight','RoundFlight','roundFlight','Flight','Rail','ListHotel','listHotel','Hotel','TWRail','TW','Domestic','CA','busCA','busUK','busUS','com','HK']:
                if source.endswith(typ):
                    source=re.sub(typ+'$','',source)
            source_flag.setdefault(source,0)
            stat_dict.setdefault(source,[0,0])
            if source_flag[source]==0:
                stat_dict[source][0]+=1
                source_flag[source]=1
            for value in error.values():
                stat_dict[source][1]+=value
    print times
    for source,time_list in stat_dict.items():
        time_list[0]=time_list[1]/float(time_list[0])
    stat_dict=sorted(stat_dict.items(),key=lambda x:x[1][0],reverse=True)
    for line in stat_dict:
        print line[0],':',line[1][0]
    '''
    for source,req_times in stat_dict.items():
        avg_times=req_times[1]/float(req_times[0])
        print source," ",avg_times
    '''
            
