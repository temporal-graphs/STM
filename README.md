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
#clone TAGBuilder and install it

git clone https://github.com/temporal-graphs/TAGBuilder.git

cd TAGBuilder/code/STMBase

mvn clean package install

cd TAGBuilder/code/TAGBuilder

mvn clean package install

git clone https://github.com/temporal-graphs/STM.git

mvn clean package
```
It generates an uber-jar in the `target` directory which can be used to generate the ITeM distributions
```
java -cp target/uber-STM-1.3-SNAPSHOT.jar gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType -input_file="input.csv" -separator="," -sampling=false -valid_etypes=1 -delta_limit=false -k_top=4 -base_out_dir=".\output\itemfreq\"
```
where `input.csv` has following format
```
1,0,2,1001
1,0,3,1002
1,0,4,1002
2,0,5,1003
```
