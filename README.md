# pinterest_cloud

## Setup:

- login credentials sent through email

- Initial user_posting_emulation mimics data treams through 3 endpoints: 
    - pinterest_data contains data about posts being updated to Pinterest
    - geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
    - user_data contains data about the user that has uploaded each post found in pinterest_data

- On AWS Console:
    - Go to parameter store & search for KeypairID, under value show the decrypted value & update hidden .pem key file, ensure *.pem is included in .gitignore
    - Next move to EC2 instances & search for userID to find associated EC2 instance, here under details you will see a field titled ```Key pair assigned at launch```, ensure local key matches this

- Once credentials have been added to local project & hidden:
    - SSH into AWS EC2: recommended to use remote explorer extension for VSCode, copy recommended command into prompt but instead of root, connect with ec2-user before @
    - example: ssh -i "xxxxxxx-key-pair.pem" ec2-user@ec2-xxxxxxxx.compute-1.amazonaws.com

## Initial EC2 Dependancies:

- Java 8:
    - Run `sudo yum install java-1.8.0`

- Kafka:
    - install & unpack kafka tgz file:
        - `wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`
        - `tar -xzf kafka_2.12-2.8.1.tgz`
        - `rm kafka_2.12-2.8.1.tgz`

## IAM Auth:
- AWS have created a package to allow developers to use IAM to connect to MSK, Kafka client can use this IAM auth to utilise AWS MSK provider, to update Kafka client:
    
    - move into libs directory, fill path here: `/home/ec2-user/kafka_2.12-2.8.1/libs`
    - install public GitHub MSK IAM package: 
    `wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar`
    - CLASSPATH is required by java to utilise imports/classes to be used when running Java based applications, in this case we need to add the PATH to `aws-msk-iam-auth-1.1.5-all.jar` to the classpath so on execution the JVM can also import our IAM authentication package
    - Move back to the users home directory, if in the libs folder then `cd ../..`
    - we need to edit the current .bashrc to add $CLASSPATH env variable
    - `nano .bashrc` then add this line `export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`
    - finally apply the changes with a quick `source .bashrc` after saving the .bashrc

## Kafka Auth:
- At the moment we have enabled the IAM MSK package however we need to edit the kafka client binary to include an ARN (Amazon Resource Name) role that specifies the exact resource for Kafka to use

- Before we move to Kafka, lets setup our AWS Role which kafka will point to in order to assume permissions
    
    - In AWS -> IAM -> Roles, search for your UserID, this role has many permissions such as topic write, topic view etc `<your_UserId>-ec2-access-role`
    - Copy the ARN string on this page, then under trust relationships -> Edit -> add principle -> IAM `
    - Paste the ARN from before & finally update the policy!

- Now lets get Kafka ready:

    - move into `kafka_2.12-2.8.1/bin` folder
    - here create a client.properties file & add code snippet from notebook, ensuring to replace ARN of the role we just updated

## Role Creation:

- Finding Bootstrap servers string & Plaintext Apache Zookeeper connection string:

    - Zookeeper is essential for Kafka, this manages the distributed computing, zookeeper tracks leader nodes, syncronisation services etc, essentially managing all brokers, producers & consumers

    - Kafka brokers manage messaging between producers & consumers, replicas are made to ensure data loss is not an issue if a broker goes down, which is tracked by the , Bootstrap maps all of the hostname:ports of the brokers within the cluster

- Now how do we find these two strings?:

    - `AWS Portal -> MSK -> pick cluster -> view client information`
    - Here we can find both bootstrap & zookeeper strings

- Alright lets make the topics!:

    - Back to the bin folder in our EC2
    - Kafka create topic command:
    `./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <topic_name>`
    - Topic names need to follow `<your_UserId>.topic_name` syntax or permission denied :(
    - On successful topic creation you'll be met with this message:
    `Created topic <UserID>.geo`

## Setting Up Connector:

- This acts as our _sink connector_ -> Allows connection & data transfer from kafka to S3 in this case
- We will first need to find our bucket on aws, under naming convention `<UserID>-bucket`

    - Move back to home directory of user & run following commands:
    - //assume admin user privileges
      `sudo -u ec2-user -i`
    - //create directory where we will save our connector 
      `mkdir kafka-connect-s3 && cd kafka-connect-s3`
    - //download connector from Confluent
      `wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip`
    - //copy connector to our S3 bucket
      `aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/`

- Great we now have a connector established but not configured, the final command above also sends a copy of this to our S3 bucket
- next step is to configure this connector:

    - On `MSK Connect -> Customised Plugin`, lets create the plugin!
    - Search for your bucket, iterate through the folders till you find a null, this is where we will make the plugin
    - Plugin must follow `<your_UserId>-plugin` syntax
    - Under `MSK Connect -> Connectors` lets press the Create Connector
    - Copy script from notebook, add USERID, and bucket field, then at the bottom of the page add your EC2-Role

## Configuring API Gateway for Batch Processing:

- API Gateway facilitates sending data from local user emulation into MSK/EC2 (god this is confusing)
Lets break this!

- Inside API Gateway (AWS):
    - In APIs, fid API associated with userID
    - On `resource tab -> Create Resource` ensure proxy resource is turned on, leave the path as root & add resource name `{proxy+}` then create the resource
    - Click this new {proxy+} and then `create method` here add:
        - POST method Type
        - HTTP Proxy
        - ANY for the HTTP method
        - Endpoint should be in this format: http://{your_EC2_DNS}/{proxy}
    - Create the method & deploy the API!

 - On EC2:
    - Install the confluent package for REST proxy to connect to EC2 Client
        - ```sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz```
        - ```tar -xvzf confluent-7.2.0.tar.gz ```
    - From root dir of EC2:
        - `cd confluent-7.2.0/etc/kafka-rest`
        - `nano kafka-rest.properties`
        - Uncomment out zookeeper.connect & zookeeper.servers
        - Replace these with Bootstrap & Zokeeper strings from earlier
        - Add code block in notebook to this file
        - Add your EC2 ARN Role ( can be found in IAM, search for userID)
    - Testing:
        - Move to `confluent-7.2.0/bin`
        - Run `./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`
        - Send Payload to API, check for 200 response & bucket to be populated with topic

## Batch Processing: Databricks:

- Import pysparksql & urllib, Pull credentials from url & query then (Don't show them xoxo)
- SET databricks formatter to False
- Create mou nt parsing in s3 bucket credentials
- Read & display dataframes based on three topic folders within S3

## Batch Processing: AWS MWAA:

AWS Managed Workflows for Apache Airflow - Simplifies scheduling for ETL scheduling i n AWS environment, scales workers & includes uilt in authentication, copy file from notebook & lets change some bits

- Filename needs to follow `<user_id>_dag.py`
- On databricks copy full path to notebook you want to schedule, add this to notebook path
- Owner is your user_id
- With DAG should be followed by your filename
- Startdate is just python's datetime method inputiing todays date
- Schedule interview is user readable @daily
- To find cluster_id go `Databricks -> compute (left hand sidebar) -> click your cluster -> on ... icon (top right) click view JSON & copy cluster_id`
- Finally upload to S3 bucket under `mwaa-dags-bucket/dags`