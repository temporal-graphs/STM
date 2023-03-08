# Structural Temporal Modeling (STM)
Networks are a fundamental and flexible way of representing various complex systems. 
Many domains such as communication, citation, procurement, biology, social media, and transportation 
can be modeled as a set of entities and relationships among them. 
Temporal networks are a specialization of general networks where temporal evolution of the system is as important to 
understand as the structure of entities and relationships. 

This code discovers Independent Temporal Motif (ITeM) in a temporal network. It takes a temporal graph of the format 
```
source,edge_type,destination,time
```
and generates various distributions using ITeM

Please contact Sumit.Purohit@pnnl.gov for any question.

```


#clone TAGBuilder and install it in <HOME> dir

git clone https://github.com/temporal-graphs/TAGBuilder.git

cd TAGBuilder/code/STMBase

mvn clean install

cd TAGBuilder/code/TAGBuilder

mvn clean install

cd <HOME>

git clone https://github.com/temporal-graphs/STM.git

cd STM

mvn clean package
```
It generates an uber-jar in the `target` directory which can be used to generate the ITeM distributions
```
java -cp target/uber-STM-1.4-SNAPSHOT.jar gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType -input_file="input.csv" -separator="," -sampling=false -valid_etypes=1 -delta_limit=false -k_top=4 -max_cores=1 -base_out_dir="./item-output/"
```
where `input.csv` has following format
```
1,0,2,1001
1,0,3,1002
1,0,4,1002
2,0,5,1003
```

It generates multiple internal files for different temporal properties. Follwoing script reads them in and generate "graph embeddings" and "node embeddings"
```
python STMGetEmbedding.py './item-output/' './emb/'
```


If you find this useful, please cite following publication
```
@article{purohit2022item,
  title={ITeM: Independent temporal motifs to summarize and compare temporal networks},
  author={Purohit, Sumit and Chin, George and Holder, Lawrence B},
  journal={Intelligent Data Analysis},
  volume={26},
  number={4},
  pages={1071--1096},
  year={2022},
  publisher={IOS Press}
}
```
