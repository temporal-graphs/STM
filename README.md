# STM
Networks are a fundamental and flexible way of representing various complex systems. 
Many domains such as communication, citation, procurement, biology, social media, and transportation 
can be modeled as a set of entities and relationships among them. 
Temporal networks are a specialization of general networks where temporal evolution of the system is as important to 
understand as the structure of entities and relationships. 

Please contact Sumit.Purohit@pnnl.gov for any question.


#clone TAGBuilder and install it

git clone https://github.com/temporal-graphs/TAGBuilder.git

cd TAGBuilder/code/STMBase
mvn clean package

cd TAGBuilder/code/TAGBuilder
mvn clean package install

git clone https://github.com/temporal-graphs/STM.git

mvn clean package
