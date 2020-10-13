#!/usr/bin/env python
# coding: utf-8

# In[183]:


import pandas as pd
import matplotlib.pyplot as plt
import pandas
import re
import matplotlib.cm as cm
import glob 
import json
import numpy as np
import sys


# In[109]:


def get_emb_list_from_json_row(json_row):
    row=[]
    for (k, v) in json_row.items():
            if isinstance(v, list):
                row.extend(v)
            else:
                row.append(v)
    return row


# In[127]:


def get_num_v_association(filePath):
    itr_w_dict = {}
    with open(filePath) as infile:
        for line in infile:
            larr = line.rstrip().split(",")
            #first 3 entries are ir,w,motifid/orbitid. rest are vertex
            num_v = len(larr) - 3
            itr_w_dict["_".join(larr[0:3])] = num_v
    return itr_w_dict


# In[130]:


def get_3rd_entry_ind(filePath):
    lemb=[]
    with open(filePath) as infile:
            for line in infile:
                larr = line.rstrip().split(",")
                lemb.append(float(larr[2]))
    return lemb


# In[169]:


# emebedding file lists:  
graph_emb_input_files = ["*ITeM_Freq.txt" , "*ITeM_Rate.txt", "*Motif_Association.txt",
                         "*Motif_Ind.txt", "*Offset_AbsCount.txt", "*Offset_Rate.txt", "*Orbit_Association.txt",
                         "*Orbit_Ind.txt", "*Vertex_Ind.txt"]

node_emb_input_files = ["*Vertex_Orbit_Frequency*","*Vertex_ITeM_Frequency*"]


# In[191]:


def get_graph_embeddings(inputpath,graph_emb_input_files):
    print("# Generating Graph Embeddings Using Following Files#")
    g_embedding = []
    TOTAL_MOTIF = 16
    TOTAL_ORBIT = 29
    for f in graph_emb_input_files:
        print(f)
        filePath = glob.glob(inputpath+"/"+f)[0]
        if f == "*ITeM_Freq.txt":
            item_freq = json.load(open(filePath))
            g_embedding.extend(get_emb_list_from_json_row(item_freq[0]))

        if f == "*ITeM_Rate.txt":
            with open(filePath) as infile:
                for line in infile:
                    g_embedding.append(float(line.rstrip()))

        if f == "*Motif_Association.txt":
            #get motif associaation from motif 0 to 15 (total 16 motif). if no value, then put zero
            # it has one line for each association in every ir, w
            itr_w_dict = get_num_v_association(filePath)

            itr=0
            w=0
            for i in range(TOTAL_MOTIF):
                g_embedding.append(itr_w_dict.get("_".join([str(itr),str(w),str(i)]),0))

        if f == "*Motif_Ind.txt":
            #get motif indepe from motif 0 to 15 (total 16 motif). if no value, then put 0
            # it has one line for each in , 3 enries, 3rd is ind
            g_embedding.extend(get_3rd_entry_ind(filePath))

        #"*Offset_AbsCount.txt"
        if f == "*Offset_AbsCount.txt":
            offset_row = json.load(open(filePath))
            # TODO: proper json
            g_embedding.extend(get_emb_list_from_json_row(offset_row))

        if f == "*Offset_Rate.txt":
            # every line is one entry
             with open(filePath) as infile:
                for line in infile:
                    off_rate = line.rstrip()
                    g_embedding.append(float(off_rate))

        if f == "*Orbit_Association.txt":
            # every line is itr,w,orbitit,nodes. 
            itr_w_dict = get_num_v_association(filePath)
            
            itr=0
            w=0
            for i in range(TOTAL_ORBIT):
                g_embedding.append(itr_w_dict.get("_".join([str(itr),str(w),str(i)]),0))

        if f == "*Orbit_Ind.txt":
            orbit_ind_json_row = json.load(open(filePath))
            g_embedding.extend(get_emb_list_from_json_row(orbit_ind_json_row))
            # TODO: proper json

        if f == "*Vertex_Ind.txt":
            g_embedding.extend(get_3rd_entry_ind(filePath))
    
    # fill zero for na, change everything to float
    tmp = pd.Series(g_embedding, dtype=object).fillna(0).tolist()
    return [float(e) for e in tmp]


# In[188]:


# get node embeddings
def get_node_embedding(inputpath,graph_emb_input_files):
    print("# Generating Node Embeddings Using Following Files#")
    for f in graph_emb_input_files:
        print(f)
        filePath = glob.glob(inputpath+"/"+f)[0]
        
        if f == "*Vertex_Orbit_Frequency*":
            vof = pd.read_csv(filePath,header=None,index_col=0,dtype=np.float64)
            
        if f == "*Vertex_ITeM_Frequency*":
            vif = pd.read_csv(filePath,header=None,index_col=0,dtype=np.float64)
            
    
    #outer join
    result = pd.concat([vof, vif], axis=1, sort=False).fillna(0)
    
    return result


# In[ ]:


def main():
    print("### Generating Graph and Node Embeddings ###\n")
    inputpath = sys.argv[1]
    outpath = sys.argv[2]
    graph_emb = get_graph_embeddings(inputpath,graph_emb_input_files)
    node_emb = get_node_embedding(inputpath,node_emb_input_files)
    
    gdf = pd.DataFrame()
    gdf = gdf.append(graph_emb)
    gdf.to_csv(outpath+"graph.emb")
    
    node_emb.to_csv(outpath+"node.emb")
    
    
if __name__ == "__main__":
    main()


# In[ ]:




