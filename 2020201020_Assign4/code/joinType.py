import os  # Last Updated : March 18, 9:46 pm # Final
import sys
from operator import itemgetter
import heapq
from datetime import datetime



orderOp=''
m_rec=100 # No. of tuples allowd in eac M blocks

M=0         #total number of blocks

r_file=''
s_file=''
r_part=0 # no of partitions in R
s_part=0  # no of partitions in S
r_heapQ=[]
flst=[]
s_flst=[]

finalLs=[]   # stores final join results


indexLst=[]  # contains indices of order by columns # indices numbers..

qLst=[]  # for adding elements in relation S as list of list

fout=0

fp1=0  # File pointers for R and S in hash join
fp2=0

r_tup=0  # Number of tuples in R
s_tup=0  # Number of tuples in S


class classHeap(object):
    def __init__(self,lst,part_no):  #lst denotes list of records splitted in a list
        self.lst = lst
        self.part_no=part_no
        

    def __lt__(self, other):
        
        for i in indexLst:
            if(self.lst[i]<other.lst[i]):
                return True




colDc={}  # stores Bytes corresponding to each column
colNamesLst=[]
readDc={}  # dict to store previous record bytes
recsize=0


def generatePartitions():
    global M
    global m_rec
    global r_file
    global s_file
    global r_tup,s_tup

    max_rec=M*m_rec
    count_rec=0
    part=0
    

    f=open(r_file,'r')
    s=''

    while True:
        content=f.readline()
        s+=content
        if content=='':
            if s!='':
                f1=open('r'+str(part),'w')
                f1.write(s)
                f1.close()
                part+=1
            break
        count_rec+=1
        r_tup+=1

        if count_rec==max_rec:
            f1=open('r'+str(part),'w')
            f1.write(s)
            f1.close()
            s=''
            part+=1
            count_rec=0
    f.close()

    r_part=part  # total partitions in r

    count_rec=0
    part=0

    f=open(s_file,'r')
    s=''

    while True:
        content=f.readline()
        s+=content
        if content=='':
            if s!='':
                f1=open('s'+str(part),'w')
                f1.write(s)
                f1.close()
                part+=1
            break
        count_rec+=1
        s_tup+=1

        if count_rec==max_rec:
            f1=open('s'+str(part),'w')
            f1.write(s)
            f1.close()
            s=''
            part+=1
            count_rec=0
    f.close()

    s_part=part  # total partitions in s relation

    return r_part,s_part



def sortPartitions(r_part,s_part):

    global indexLst

    global r_file
    global s_file
    
    

    # ----------------------------------- For R relation ------------------------------------------------------------------

    startPart=0
    #colString=''
    indexLst=[1]
   
    #indexLst=indLst  # global variable of indexLst updated

    while startPart<r_part:
        
        tupleLst=[]
        f=open('r'+str(startPart),'r')
        while True:
            #colString=''
            content=f.readline()
            if content=='':
                break
            #content=content.replace('\n',' \n')  #Removed Feb 4
            tempLst=[]
            tempLst=content.split()
            tupleLst.append(tempLst)
            tempLst=[]
          

        tupleLst.sort(key=itemgetter(*indexLst))  # sort in ascending order  # for every partition
        
        f.close()


        f=open('r'+str(startPart),'w')

        for i in tupleLst:
            f.write((' ').join(i))
            f.write('\n')  
        f.close()
        
        startPart+=1
    

    # ------------------------------ For S relation-------------------------------------------------------------

    startPart=0
    #colString=''
    indexLst=[0]

    #indexLst=indLst  # global variable of indexLst updated

    while startPart<s_part:
        
        tupleLst=[]
        f=open('s'+str(startPart),'r')
        while True:
            #colString=''
            content=f.readline()
            if content=='':
                break
         
            tempLst=[]
            tempLst=content.split()
            tupleLst.append(tempLst)
            tempLst=[]
          

        tupleLst.sort(key=itemgetter(*indexLst))  # sort in ascending order  # for every partition
        
        f.close()


        f=open('s'+str(startPart),'w')

        for i in tupleLst:
            f.write((' ').join(i))
            f.write('\n')  
        f.close()
        
        startPart+=1
        


def writeOutputBuffer():
    global fout
    global finalLs

    #fout=open(r_file+'_'+s_file+'_'+'join.txt','w')
    for op in finalLs:
        fout.write(op)
        fout.write('\n')
    finalLs=[]  # empty the list




def mergeJoinOpen():  # implementation of open()
    global r_file
    global s_file
    global r_part
    global s_part

    r_part,s_part=generatePartitions()

    sortPartitions(r_part,s_part)  # sort the partitions




def R_init_heap():
    
    global r_part
    global m_rec
    global indexLst

    #r_heapQ=[]
    #flst=[]   # keeps list of file pointers
    num=0
    tup_no=0

    indexLst=[1]  # since 1st column is y val in R relation
   
    while(num<r_part):  # for all R partitions(sublists), the block contents are pushed to heap
        fp=open('r'+str(num),'r')
        flst.append(fp)
        tup_no=0
        while tup_no<m_rec:
            content=flst[num].readline()
            if(content==''):
                break
            readLst=content.split()
            heapq.heappush(r_heapQ, classHeap(readLst,num))
            tup_no+=1


        num+=1
    




def R_next_record():  # Get next min ymin recored using min heap

    global indexLst
    
    num=0
    indexLst=[1]

    #while(num!=r_part):
    
    #print(r_heapQ)

    if len(r_heapQ)==0:
        return ''
        
    obj=heapq.heappop(r_heapQ)
    resLst=obj.lst
    colString=(' ').join(resLst)
    
    content=flst[obj.part_no].readline()
    if(content==''):
        return colString
    readLst=content.split()
    heapq.heappush(r_heapQ, classHeap(readLst,obj.part_no))


    return colString


def S_init_heap():
   
    global s_part
    global m_rec

    num=0
    tup_no=0

    temp=[]
   
    while(num<s_part):  # for all R partitions(sublists), the block contents are pushed to heap
        fp=open('s'+str(num),'r')
        s_flst.append(fp)
        tup_no=0
        while tup_no<m_rec:
            content=s_flst[num].readline()
            if(content==''):
                break
            temp.append(content)
            tup_no+=1
        qLst.append(temp)  # qLst is a list of lists storing records of S relation
        temp=[]

        num+=1




def mergeGetNext():
    global m_rec
    global finalLs
    global M

    #print(str(r_tup),str(s_tup))
    #print(M)

    # Condition to check for Memory contraints:

    R_b=(r_tup+m_rec-1)//m_rec
    S_b=(s_tup+m_rec-1)//m_rec

    if R_b+S_b>M*M:
        sys.exit('Insufficient memory to accomodate relations R and S')

    R_init_heap()
    S_init_heap()

    

    rec=R_next_record()  # get complete record from R conaining y_min
        
    while(rec!=''):

        r_ymin=rec.split()[1]
        r_ymin=r_ymin.replace('\n','')  # We get y min value from R

        for s_rec_lst in qLst:  # list of lists
            s_ind=0
            while s_ind<len(s_rec_lst):

                if len(s_rec_lst[s_ind])==0:
                    break

                if s_ind==len(s_rec_lst)-1:

                    s_par=qLst.index(s_rec_lst)
                    content=s_flst[s_par].readline()
                    if content!='':
                        s_rec_lst.append(content)

                sval_y=s_rec_lst[s_ind].split()[0]

                if r_ymin==sval_y:
                    
                    finalLs.append(rec+' '+s_rec_lst[s_ind].split()[1])
                    if len(finalLs)==m_rec:
                        writeOutputBuffer()  # write to output buffer

                    s_ind+=1
                
                elif r_ymin>sval_y:
                    s_rec_lst.remove(s_rec_lst[s_ind])

                    s_par=qLst.index(s_rec_lst)  # get partition number

                    content=s_flst[s_par].readline()
                    if content!='':
                        s_rec_lst.append(content)
                
                else:
                    break

                
        
        rec=R_next_record()

    writeOutputBuffer()


def mergeClose():
    # Close all the open pointer files
    global r_part
    global s_part
    global fout
    global flst
    global s_flst

    
    for i in flst:
        i.close()

    for j in s_flst:
        j.close()
    
    fout.close()

    for i in range(r_part):
        os.remove('r'+str(i))
    
    for i in range(s_part):
        os.remove('s'+str(i))

def hash_fun(s):
    global M

    p = 31
    m = M-1
    p_pow = 1
    hash_val = 0
 
    for i in range(len(s)):
        hash_val = ((hash_val + (ord(s[i]) - ord('a') + 1) * p_pow) % m)
        p_pow = (p_pow * p) % m
 
    return int(hash_val)

def build_hash_fun(s): # to be used in second phase
    global M
    p = 101
    m = M-1
    p_pow = 1
    hash_val = 0
 
    for i in range(len(s)):
        hash_val = ((hash_val + (ord(s[i]) - ord('a') + 1) * p_pow) % m)
        p_pow = (p_pow * p) % m
 
    return int(hash_val)

def readFile(fileName):
    tup_cnt=0
    fread=open(fileName,'r')
    while True:
        content=fread.readline()
        if content=='':
            break
        tup_cnt+=1
    
    fread.close()
    return tup_cnt


def hashJoinOpen():
    global r_file
    global s_file
    global M
    global flst
    global s_flst
    global fp1,fp2
    global m_rec

    r_hash_tup=readFile(r_file)
    s_hash_tup=readFile(s_file)

    R_b=(r_hash_tup+m_rec-1)//m_rec
    S_b=(s_hash_tup+m_rec-1)//m_rec

    #print(r_hash_tup)
    #print(s_hash_tup)

    if min(R_b,S_b)>M*M:
        sys.exit('Insufficient memory to accomodate relations R and S')

    for i in range(M-1):
        f=open('r'+str(i),'w')
        flst.append(f)

    fp1=open(r_file,'r')

    while True:
        content=fp1.readline()
        if content=='':
            break
        r_yval=content.split()[1]
        #r_yval=r_yval.replace('\n','')

        bucket=hash_fun(r_yval)
        flst[bucket].write(content)
        flst[bucket].flush() # flush in r


    #fp1.close()  # Commented Mar 16

    for i in range(M-1):
        f=open('s'+str(i),'w')
        s_flst.append(f)

    fp2=open(s_file,'r')

    while True:
        content=fp2.readline()
        if content=='':
            break
        s_yval=content.split()[0]
       
        bucket=hash_fun(s_yval)
        s_flst[bucket].write(content)
        s_flst[bucket].flush() # flush in s
        
    #fp2.close()


def buildAndProbe(rf,sf,flag):  # r and s are individual partitions
    
    global finalLs
    global m_rec
    global M
    countTup=0

    hashDc={}  # maintaining hash table

    for j in range(M-1):
        hashDc[j]=[]

    
    fbuild=open(rf,'r')
    while True:
        #print('In while')
        content=fbuild.readline()
        if content=='':
            break
        countTup+=1
        if flag==0:
            r_yval=content.split()[1]
        else:
            r_yval=content.split()[0]

        #r_yval=r_yval.replace('\n','')

        bucket=build_hash_fun(r_yval)
        #if len(hashDc[bucket])==0:
            #countTup+=1
        hashDc[bucket].append(content)
    
    fbuild.close()

    #print('Count tuples is',countTup)


    avlSlots=m_rec*(M-1)-countTup
    #print('Total number of avl slots:',avlSlots)

    

    if avlSlots<=0:
        sys.exit('Hash Join not possible since no available slots are there to load other relation')
    
    probeLst=[]
    cnt=0
    writeFlag=0
    
    fprobe=open(sf,'r')
    while True:
        cnt=0
        while cnt<avlSlots:
            content=fprobe.readline()
            if content=='':
                writeFlag=1
                break
            probeLst.append(content)
            cnt+=1
        
        
        for val in probeLst:
            if flag==1:  # denotes s partition
                r_yval=val.split()[1]
            else:
                
                r_yval=val.split()[0]   # val of y in probe lst
                
                #r_yval=r_yval.replace('\n','')

            bucket=build_hash_fun(r_yval)
            build_yval=''


            for rec in hashDc[bucket]:
                if flag==1:  # denotes s partition  ie s<r and rec=s
                    build_yval=rec.split()[0]
                else:
                    build_yval=rec.split()[1]
                    
                    #build_yval=build_yval.replace('\n','')
            
                if r_yval==build_yval:
                    if flag==0:   # rec belongs to r  ie r<s
                        rec=rec.split()
                        new_rec=' '.join(rec)+' '+val.split()[1]
                    else:
                        val=val.replace('\n','')
                        new_rec=val+' '+rec.split()[1]
                        #writeOutputBuffer()

                    finalLs.append(new_rec)

                    if len(finalLs)==m_rec:
                        writeOutputBuffer()

        if writeFlag==1:
            writeOutputBuffer()
            break

        probeLst=[]

    fprobe.close()
    




def hashGetNext():
    global r_file
    global s_file


    for i in range(M-1):
        
        r_fileSize=os.path.getsize('r'+str(i))
        s_fileSize=os.path.getsize('s'+str(i))

        if r_fileSize==0 or s_fileSize==0:
            continue

        if r_fileSize<s_fileSize:
            buildAndProbe('r'+str(i),'s'+str(i),0)
        else:
            buildAndProbe('s'+str(i),'r'+str(i),1)


def hashClose():
    
    # Close all the open pointer files
    global M
    global fout
    global flst
    global s_flst
    global fp1,fp2

    fp1.close()  # Close R and S file pointers opened during hash Join
    fp2.close()
    
    for i in flst:
        i.close()

    for j in s_flst:
        j.close()
    
    fout.close()

    for i in range(M-1):
        os.remove('r'+str(i))
    
    for i in range(M-1):
        os.remove('s'+str(i))


        
    

def processPhases(r_file,s_file,joinType):

    global r_part
    global s_part
    global fout

    print('#### started execution')

    begin_time=datetime.now()
    print('Start Time: ',begin_time)

    r_out=''
    ind=r_file.rfind('/')

    if ind!=-1:
        r_out=r_file[ind+1:]
    else:
        r_out=r_file
    

    s_out=''
    ind=s_file.rfind('/')

    if ind!=-1:
        s_out=s_file[ind+1:]
    else:
        s_out=s_file




    if joinType=='sort':
        
        mergeJoinOpen()
        fout=open(r_out+'_'+s_out+'_'+'join.txt','w')
        mergeGetNext()
        mergeClose()
        
    
    elif joinType=='hash':
        
        hashJoinOpen()
        fout=open(r_out+'_'+s_out+'_'+'join.txt','w')
    
        hashGetNext()
        hashClose()

    else:
        sys.exit('Invalid join operation entered....')
    
    end_time=datetime.now()
    print('End Time: ',end_time)

    print('Time elapsed: '+str(end_time-begin_time))


# Taking command line arguments

if(len(sys.argv)<4):
    sys.exit('Insufficient parameters passed')



r_file=sys.argv[1]
s_file=sys.argv[2]
joinType=sys.argv[3]   

M=int(sys.argv[4])


processPhases(r_file,s_file,joinType)


