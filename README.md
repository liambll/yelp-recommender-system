# Recommender System and Visualization with Yelp data
- Collaborative Filtering Recommender System (Matrix Factorization, Item-Similarity) based on restaurant detail and user rating datasets from Yelp
- Web-based demo to demonstrate and visualize the recommender system’s result
- Technologies: Python, Spark, MLlib, Cassandra, Hadoop Cloudera Distribution, Google Map API, D3.js

## I. Data Processing
1. readjson.py converts JSON source dataset to tabular csv format, and process_business_checkin.py handles normalization of check-in data. These processed data are located at /user/llbui/bigbang/ HDFS folder.

2. load_cassandra.py load data to Cassandra
- Load User table:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --master=yarn-client load_cassandra.py /user/llbui/bigbang/yelp_academic_dataset_user.csv bigbang yelp_user

- Load Business table:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --master=yarn-client load_cassandra.py /user/llbui/bigbang/yelp_academic_dataset_business.csv bigbang yelp_business

- Load Business_Checkin table:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --master=yarn-client load_cassandra.py /user/llbui/bigbang/yelp_business_checkin.csv bigbang yelp_business_checkin

- Load Review table:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --master=yarn-client load_cassandra.py /user/llbui/bigbang/yelp_academic_dataset_review.csv bigbang yelp_review

## II. Data Analysis
1. Model Evaluation and Selection:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5 model_selection.py /user/suchitav/output-425
In the output path /user/suchitav/output-425, following model parameters are saved:
- Best rank
- Best iteration
- Best regularizer
- Train error
- Test error

2. Predicting User ratings:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5 --driver-memory 2g --executor-memory 4g user_pred_ratings.py /user/suchitav/output-425/part-00000

- /user/suchitav/output-425 contains the saved model parameters
- Cassandra table bigbang.user_pred_ratings contains all prediction outputs

3. Restaurant recommendations:
spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5 --driver-memory 2g --executor-memory 4g restaurant_recommendation.py

- Cassandra table bigbang.user_pred_ratings contains all predictions
- Cassandra tables bigbang.recommendations and bigbang.recommendations_checkin contains output for top 20 restaurants and their check-in information for each user in each state.

### III. Live Demo Website: User Interface and Visualization Live-Demo:
Note: The live-demo will work only within SFU Campus as there will be a firewall issue if tried to access from different internet service other than SFU wifi.
1. Start the webserver:
- Please go to the below path. Node exe and the scripts are placed here.
        cd /home/rmathiya/nodejs/node-v6.9.1-linux-x64/bin/
- Execute the below command.
        ./node scripts/createServer.js
        
- The below messages should be displayed which means that the server is running.
        Cassandra connected
        Server listening on: http://gateway.sfucloud.ca:8000
        
2. Launch the live-demo website?
- Please launch our restaurant recommendation website by accessing the below URL.
        http://gateway.sfucloud.ca:8000/
        
- Enter the userid and password. Below are some of the userids to test with. Password is always 12345678 for demo purpose.
* hc4D2WkjKG8mk1C9e0fsQQ
* iAAj55EL3coC2rYtXsbM5g
* Q7aDwMVp4B2E3zdAf0dsSg
* PQSvLVLMqo-Wiqlb_rTlPA
* f-PNqJwmcWkyRNTUpMJ8Aw
* 5SKmZ6spwRE__V0uI8YWDg
* WgyDBAY2QXVD4GaudWQDcg
* _sKocx_OjrQrLnGYNMuacg
* uP04N5nsDbrRhCwa3LCyZw
If the userid/password is invalid, an appropriate error message will get displayed.

- After loggin in, the page will show the visualization of restaurant recommendations. The top 20 recommended restaurants are shown for the user for the province Ontario, Canada by default. Similarly for 10 different
provinces by selecting the option from the dropdown menu on the top left corner.
With mouse over the red markers, the details of the restaurants can be seen.
On clicking more details, ratings of the recommended restaurants can be seen in bar chart. By clicking on any restaurant in bar chart, the checkin details (24 x 7) are dispalyed for that restaurant.

3. Stop the server:
Once the testing is completed, please press ctrl + c to terminate the server.
