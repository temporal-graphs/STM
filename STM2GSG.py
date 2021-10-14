#!/usr/bin/env python
# coding: utf-8

# In[123]:


from STMGetEmbedding import add_embedding_from_JSON
import glob
import json
import sys


# In[116]:


# Write GSG JSON file. GSG code is available at https://github.com/holderlb/graph-stream-generator
def writeGSG(inputpath,outgsgfile):
    
    #get duration of the input graph
    duration = 0
    filePath = glob.glob(inputpath + "/*WindowTime.txt")[0]
    with open(filePath) as f:
        for line in f:
            if line.startswith("#duration"):
                duration = int(line.strip().split(",")[1])
    
    #get ITeM freq
    filePath = glob.glob(inputpath + "/*_ITeM_Freq.txt")[0]
    TOTAL_MOTIF = 16
    TOTAL_ORBIT = 29
    g_emb_dict={}
    item_freq = json.load(open(filePath))
    gsg_json = {"numStreams": "3","secondsPerUnitTime": "1","startTime": "2021-01-01 00:00:00","outputTimeFormat": "units",
          "duration":str(duration), "outputFilePrefix": "STM_","patterns":[]}
    
    mid=0
    T = "true"
    F = "false"
    gsg_json["patterns"] = []
    for row in item_freq:
        for (k, v) in row.items():
            if k == "m0":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T)],[("e0","v0","v1"),("e1","v0","v1")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",F),("v1",T)],[("e0","v0","v1"),("e1","v0","v1")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[2])/duration,[("v0",F),("v1",F)],[("e0","v0","v1"),("e1","v0","v1")]))
                mid += 1

            if k == "m1":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T)],[]))
                mid += 1

            if k == "m2":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T)],[("e0","v0","v1")]))
                mid += 1

            if k == "m3":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",F),("v1",F)],[("e0","v0","v1"),("e1","v0","v1")]))
                mid += 1

            if k == "m4":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T)],[("e0","v0","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",F)],[("e0","v0","v0")]))
                mid += 1

            if k == "m5":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",F),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",F),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v0")]))
                mid += 1

            if k == "m6":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",F),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",F),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2")]))
                mid += 1

            if k == "m7":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v1"),("e3","v1","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",F),("v2",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2"),("e3","v1","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2"),("e3","v1","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",F),("v1",F),("v2",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v0","v2"),("e3","v1","v0")]))
                mid += 1

            if k == "m8":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T),("v3",T)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v3"),("e3","v3","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",T),("v2",T),("v3",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v3"),("e3","v3","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",T),("v2",F),("v3",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v3"),("e3","v3","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",T),("v1",F),("v2",F),("v3",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v3"),("e3","v3","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_4__"+str(mid),T,float(v[4])/duration,[("v0",F),("v1",F),("v2",F),("v3",F)],[("e0","v0","v1"),("e1","v1","v2"),("e2","v2","v3"),("e3","v3","v0")]))
                mid += 1

            if k == "m9":
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T)],[("e0","v0","v1"),("e3","v1","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",F)],[("e0","v0","v1"),("e3","v1","v0")]))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",F),("v1",F)],[("e0","v0","v1"),("e3","v1","v0")]))
                mid += 1

            if k == "m10":
                einfo = [("e0","v0","v1"),("e1","v0","v2"),("e2","v0","v3")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T),("v3",T)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",T),("v2",T),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",T),("v2",F),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",T),("v1",F),("v2",F),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_4__"+str(mid),T,float(v[4])/duration,[("v0",F),("v1",F),("v2",F),("v3",F)],einfo))
                mid += 1

            if k == "m11":
                einfo = [("e0","v1","v0"),("e1","v2","v0"),("e2","v3","v0")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T),("v3",T)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_1__"+str(mid),T,float(v[1])/duration,[("v0",T),("v1",T),("v2",T),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_2__"+str(mid),T,float(v[2])/duration,[("v0",T),("v1",T),("v2",F),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_3__"+str(mid),T,float(v[3])/duration,[("v0",T),("v1",F),("v2",F),("v3",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_4__"+str(mid),T,float(v[4])/duration,[("v0",F),("v1",F),("v2",F),("v3",F)],einfo))
                mid += 1

            if k == "m12":
                einfo = [("e0","v0","v1"),("e1","v0","v2")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",F),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",F),("v1",F),("v2",F)],einfo))
                mid += 1

            if k == "m13":
                einfo = [("e0","v1","v0"),("e1","v2","v0")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",F),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",F),("v1",F),("v2",F)],einfo))
                mid += 1

            if k == "m14":
                einfo = [("e0","v0","v1"),("e1","v1","v2")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",T)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",T),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",F),("v2",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",F),("v1",F),("v2",F)],einfo))
                mid += 1

            if k == "m15":
                einfo = [("e0","v0","v1")]
                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",T),("v1",F)],einfo))
                mid += 1

                gsg_json["patterns"].append(make_pat(f"{k}_0__"+str(mid),T,float(v[0])/duration,[("v0",F),("v1",F)],einfo))
                mid += 1


        
        with open(outgsgfile,"w") as outfile:
            json.dump(gsg_json, outfile, indent=2)
    


# In[2]:


#inputpath = "D:/localcode/STM/data/emailEU11.0_tmp"


# In[23]:


#filePath = glob.glob(inputpath + "/*_ITeM_Freq.txt")[0]


# In[118]:


def make_v(id_,new_,):
    return {"id":str(id_),"new":new_, "attributes":{}}


# In[120]:


def make_e(id_,s,t,di = "true", min_off=0,max_off=0,streamNum = 1):
    return {"id":str(id_),"source":str(s),"target":str(t),"directed":di,
            "minOffset":str(min_off),"maxOffset": str(max_off),"streamNum":streamNum,"attributes":{}}


# In[59]:


def make_v_list(vlist):
    allv = []
    for entry in vlist:
        allv.append(make_v(entry[0],entry[1]))
    return allv


# In[66]:


def make_e_list(elist):
    alle = []
    for entry in elist:
        alle.append(make_e(entry[0],entry[1],entry[2]))
    return alle


# In[73]:


def make_pat(pid,track,prob,vinfo,einfo):
    newp = {}
    newp["id"] = pid
    newp["track"] = track
    newp["probability"] = str(prob)
    newp["vertices"] = make_v_list(vinfo)
    newp["edges"] = make_e_list(einfo)
    return newp


# In[126]:


def main():
    
    help_str = "python STM2GSG.py <input_dir_path> <output_json_file_path>"
    if len(sys.argv) < 3:
        print(help_str)
        sys.exit()
    if sys.argv[1] == "help":
        print(help_str)
        sys.exit()
    in_dir_path = sys.argv[1]
    out_json_path = sys.argv[2]
    writeGSG(in_dir_path,out_json_path)
    print("STM2GSG:SUCCESS")


# In[ ]:



if __name__ == "__main__":
    main()


# In[ ]:




