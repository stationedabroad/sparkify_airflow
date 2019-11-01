# sparkify_airflow
This folder includes contents on a basic implementation project for airflow, using a pipeline which serves using the following:
  * **S3 connector source data reads json from S3 buckets**
  * **AWS Redshift connector target writes to staging and fact/dimension tables**
  * **Basic automation of data quality checks**

The basic premise aims to carry input source data written to S3 buckets as json files, on a fixed schedule to an AWS Redshift staging area.  From here data is used to populate Fact and Dimension tables to satisfy a star schema pattern of data-warehouse design.  Some key decisions led to this current implementation which shall be detailed below.

# Design
* Rather than implementing a load_fact and load_dimension task operator, I used one (modelled as class **LoadFactDimensionOperator**).  This is due to the fact it served my purpose to bundle together two very similar functions as the *COPY FROM* sql statemnet used for both for very similar.  Hence I used parameters to the class to allow a dynamic *COPY FROM* to be created for facts and dimensions.
* I created a base operator whch abstracted away the connection details (*AWS logins* and *Redshfit* cluster details).

