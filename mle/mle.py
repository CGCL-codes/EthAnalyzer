import random
import pandas as pd
import numpy as np
#数据读取
add=pd.read_csv("addr4.csv")
tra=pd.read_csv("transaction.csv")
ip_code=add["ip_code"].values
label=add["label"].values
Ah=ip_code[np.where(label<4)]
Ch=label[np.where(label<4)]
Ar=ip_code[np.where(label>3)]
thr=0.0001

send_ip=tra["send_ip"].values
receive_ip=tra["receive_ip"].values

ip=set(list(send_ip))
ip1=set(list(receive_ip))
ip=ip1 | ip
ip=list(ip)
ip.sort()


Ls=np.zeros((len(ip),len(ip)))
Lr=np.zeros((len(ip),len(ip)))

for i in range(send_ip.shape[0]):
    ind1=ip.index(send_ip[i])
    ind2=ip.index(receive_ip[i])
    Ls[ind1,ind2]=1
    Lr[ind2,ind1]=1
def calculateReceive_SendMatrix(Ah,Ch):
    sc=[]
    rc=[]
    ah=list(Ah)
    ch=list(Ch)
    for i in range(send_ip.shape[0]):
        if send_ip[i] in ah:
            sc.append(ch[ah.index(send_ip[i])])
        else:
            sc.append(4)
        if receive_ip[i] in ah:
            rc.append(ch[ah.index(receive_ip[i])])
        else:
            rc.append(4)
    sc=np.array(sc)
    rc=np.array(rc)
    sr=np.concatenate((sc.reshape(225714,1),rc.reshape(225714,1)),axis=1)
    p=np.zeros((3,3))
    q=np.zeros((3,3))
    for i in range(3):
        tmpn=sc[np.where(sc==i+1)].shape[0]
        for j in range(3):
            a=sr[:,0]==i+1
            b=sr[:,1]==j+1
            c=a & b
            tmpm=sr[c,:].shape[0]
            p[i,j]=tmpm/tmpn
    for i in range(3):
        tmpn=rc[np.where(rc==i+1)].shape[0]
        for j in range(3):
            a=sr[:,0]==j+1
            b=sr[:,1]==i+1
            c=a & b
            tmpm=sr[c,:].shape[0]
            q[i,j]=tmpm/tmpn
    
    return p,q

Am=list(Ah)
Cm=list(Ch)
ar=list(Ar)
aip=np.array(ip)

def predictAccountType(p,q,node):
    t=[]
    for i in range(3):
        tmp=1
        for j in range(3):
            tmp=tmp*p[i,j]
        t.append(tmp)
    t1=[]
    for i in range(3):
        tmp=1
        for j in range(3):
            tmp=tmp*q[j,i]
        t1.append(tmp)
    t2=[]
    for i in range(3):
        t2.append(t[i]*t1[i])
    type1=t2.index(max(t2))+1
    
    return type1

p,q=calculateReceive_SendMatrix(np.array(Am),np.array(Cm))
while len(ar)>0 :
    for node in ar:
        #取出行
        ne1=aip[np.where(Ls[ip.index(node),:]==1)]
        ne2=aip[np.where(Lr[ip.index(node),:]==1)]
        count=0
        
        for i in range(ne1.shape[0]):
            if ne1[i] in Am:
                count += 1
        
        for i in range(ne2.shape[0]):
            if ne2[i] in Am:
                count += 1
        if count/(2*Ls.shape[0])>=thr:
            type1 = predictAccountType(p,q,node)
            Am.append(node)
            Cm.append(type1)
            ar.remove(node)
            
            print("node:",node,"  类型 ：",random.randint(0, 4))
            
            
        
        
        
        
    
        