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
    - SSH into AWS EC2: recommended to use remote explorer extension for AWS, copy recommended command into prompt but instead of root, connect with ec2-user before @
    - example: ssh -i "xxxxxxx-key-pair.pem" ec2-user@ec2-xxxxxxxx.compute-1.amazonaws.com

## Initial EC2 Dependancies:

- Java 8:
    - Run `sudo yum install java-1.8.0`

- Kafka:
    - install & unpack kafka tgz file:
        - `wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`
        - `tar -xzf kafka_2.12-2.8.1.tgz`
        - `rm kafka_2.12-2.8.1.tgz`

