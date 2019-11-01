# sparkify_airflow
This folder includes contents on a basic implementation project for airflow, using a pipeline which serves using the following:
  * **S3 connector source data reads json from S3 buckets**
  * **AWS Redshift connector target writes to staging and fact/dimension tables**
  * **Basic automation of data quality checks**

The basic premise aims to carry input source data written to S3 buckets as json files, on a fixed schedule to an AWS Redshift staging area.  From here data is used to populate Fact and Dimension tables to satisfy a star schema pattern of data-warehouse design.  All using the *pipeline modelling* and *scheduling* of **apache airflow**.  Some key decisions led to this current implementation which shall be detailed below.

The data flow appears like this in the graph view screen in airflow, with the *stage_songs* and *stage_events* processes moving data between S3 and Redshift staging area, and the *Load\** jobs populating the Redshift Facts and Dimensions:
![flow_diagram]

## Design Decisions
* Rather than implementing a load_fact and load_dimension task operator, I used one (modelled as class **LoadFactDimensionOperator**).  This is due to the fact it served my purpose to bundle together two very similar functions as the *COPY FROM* sql statemnet used for both for very similar.  Hence I used parameters to the class to allow a dynamic *COPY FROM* to be created for facts and dimensions.
* I created a base operator whch abstracted away the connection details (*AWS logins* and *Redshfit* cluster details).  this saved a great deal of repeat code and followed the DRY principle.  Hence, the load fact and dimensions operators as well as the data quality check operator use the **S3RedshiftConnector** class as a parent and implement the execute function with custom code.
* I did not use the *truncate* method for writing dimension tables.  This is because I believe this is incorrect; dropping dimension tables each time will not guarantee me the dimension table keys (if they are custom keys as they are and should be in most start schea designs) will always be the same.  Hence the dimension keys become aligned with the facts keys on all future loads.  Dropping the dimension table for me does not serve any purpose.
* I used the *SqlQueries* class to good affect, using  it to manage the data quality checks, inserts and table columns.  In this sense its a one stop shop for the ETL process, one file which can be managed and maintained when needing to update fields, or quality checks.

[flow_diagram]: airflow_sparkify_pipeline.png
