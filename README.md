# pinterest_cloud

- login credentials sent through email

- Initial user_posting_emulation mimics data treams through 3 endpoints: 
    - pinterest_data contains data about posts being updated to Pinterest
    - geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
    - user_data contains data about the user that has uploaded each post found in pinterest_data

- On AWS Console:
    - Go to parameter store & search for KeypairID, under value show the decrypted value & update hidden .pem key file, ensure *.pem is included in .gitignore
    - Next move to EC2 instances & search for userID to find associated EC2 instance, here under details you will see a field titled ```Key pair assigned at launch```, ensure local kep matches this