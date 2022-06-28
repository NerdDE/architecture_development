# Setting Up a Dataproc Cluster

## Theory

### **Distributed Systems**

_Distributed processing_ is the use of more than one processor to perform the processing for an individual task. This also referred as horizontal scaling processing and it's used when single machine resources are not enough to process huge amounts of data.

### Spark

[Apache Spark™](https://spark.apache.org/) is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

**Architecture**
![alt text](https://spark.apache.org/docs/latest/img/cluster-overview.png)

### What is Dataproc?

Managed Apache Spark and Apache Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning.

Cluster automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them.

### How does it work? 

It decouples storage from compute. We process data that is already in bucket. 
Dataproc process it from here and outputs it to a data lake/DWH typically. 




## Practice

1. **Permissions**
- Create service account with the following permissions/roles: 
	- BigQuery Admin

	- Composer Administrator

	- Dataproc Administrator

	- Storage Admin

	- Storage Object Admin

- Create `JSON` key and download it.

2. **Storage**
- Create bucket (assign a *specfic* name).

-  `pip install -r requirements.txt`

- Adapt `ingest_script.py` with your `project_id`, `service_account` path and `bucket` name (and run it locally).

- Create `code` folder.

3. **Processing**
- Upload `pyspark_job.py` into `<your_bucket>/code/`
- Go to the  [Dataproc UI](https://console.cloud.google.com/dataproc/clusters)
- Enable Dataproc API

- **Create Cluster**
	- Enable API interactions 
	-  Single Node
	-  **Image**: `2.0 (Ubuntu 18.04 LTS, Hadoop 3.2, Spark 3.1)`
	-  Check `enable component gateway` box
	-  Components: `Jupyter notebook` `Docker`

- Submit our first job!