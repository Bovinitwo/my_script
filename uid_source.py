# -*- coding: utf-8 -*-

import sys
import os
import time
import datetime
import re
import json

if __name__ =='__main__':
    flight_dict={}
    hotel_dict={}
    source_dict={}
    with open('/home/liuyu/python_script/uid_source/MD5_5') as f:
        while True:
            each_content = f.readline()
            if each_content:
                each_content=each_content.split('||||||')
                if len(each_content) != 10:
                    continue
                if each_content[7]!='NULL' and each_content[7]!='':
                    if each_content[2]=='flight':
                        flight_dict.setdefault(each_content[7],[0,0,0])
                        if each_content[9].find('UCCN')!=-1:
                            flight_dict[each_content[7]][1]+=1
                        if each_content[9].find('MTCN')!=-1:
                            flight_dict[each_content[7]][2]+=1
                        else:
                            flight_dict[each_content[7]][0]+=1
                    if each_content[2]=='hotel':
                        hotel_dict.setdefault(each_content[7],[0,0,0])
                        if each_content[9].find('UCCN')!=-1:
                            hotel_dict[each_content[7]][1]+=1
                        if each_content[9].find('MTCN')!=-1:
                            hotel_dict[each_content[7]][2]+=1
                        else:
                            hotel_dict[each_content[7]][0]+=1

                '''
                try:
                    if each_content[7]=='NULL' or each_content[7]=='' or each_content[7] == 'hsv009':
                        source=each_content[8]
                        for typ in ['Car','Bus','MultiFlight','multiFlight','RoundFlight','roundFlight','Flight','Rail','ListHotel','listHotel','Hotel']:
                            if source.endswith(typ):
                                source=re.sub(typ+'$','',source)
                        source_dict.setdefault(source,[0,0,0])
                        if each_content[9].find('UCCN')!=-1:
                            source_dict[source][1]+=1
                        if each_content[9].find('MTCN')!=-1:
                            source_dict[source][2]+=1
                        else:
                            source_dict[source][0]+=1 
                except Exception,e:
                    print str(e)
                    continue
                '''
            else:
                break                     
    '''
    print source_dict

    for source,i_list in source_dict.items():
        print source
        print "\tCN:",source_dict[source][0]
        print "\tUCCN:",source_dict[source][1]
        print "\tMTCN:",source_dict[source][2]

    '''
    print flight_dict
    flight_dict=sorted(flight_dict.items(),key=lambda x:x[1][0],reverse=True)
    for data in flight_dict:
        print data[0]
        print "\tCN",data[1][0] 
        print "\tUCCN",data[1][1] 
        print "\tMTCN",data[1][2]
    hotel_dict=sorted(hotel_dict.items(),key=lambda x:x[1][0],reverse=True)
    for data in hotel_dict:
        print data[0]
        print "\tCN",data[1][0] 
        print "\tUCCN",data[1][1] 
        print "\tMTCN",data[1][2] 
