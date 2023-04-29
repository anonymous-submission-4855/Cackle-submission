# Cackle Submission

This repository contains software artifacts from the Cackle Project. It has been anonymized for the reviewers. Below is a brief description of the folders. The model is implemented in Python. Modified Starling is written in C++ with code generation tool written in Python. Other components implemented in Rust. 

Identifying information, including AWS buckets, and other private information have been removed, so the code, with the exception of the model, may not compile or execute as is. 

- **cackle-cache**: The caching node key-value store
- **cackle-coordinator**: The coordinator implementation. Contains code for executing queries, recording history, and adjusting compute and cache allocation
- **cackle-model** : The model implementation
	- shuffle\_model\_and\_workload\_gen.py : workload generation tool and shuffle modeling. (should be run before other tools)
	- compute\_model.py : Compute modeling
	- work\_delaying\_compute.py : Work delaying model implementation
- **cackle-starling**: The modified Starling implementation that allows shuffling through the cache nodes. 
- **databricks-experiment**: Databricks experiment code



