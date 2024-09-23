#Domain Update Pipeline


This pipeline automates the process of updating email domains in LaunchDarkly (LD) based on data fetched from AWS S3. The pipeline retrieves domain lists stored in S3, processes them, and updates LD with the latest data. It includes functions for fetching, filtering, and updating domains through API calls.

---

**Pipeline Overview**
##The pipeline follows this basic flow:



                                          ------ s3Services -----> index
                                          |
      lambda/handler.handler -----> processLdRequest --- utilsServices  --->index
                                          |
                                           ------ ldServices -----> index

* Handler: The entry point (Lambda function) that triggers the pipeline.
* processLdRequest: The main workflow that coordinates fetching data from S3 and updating LD.
* s3Services: Service functions that interact with AWS S3 to list and retrieve domain data.
* utilsServices: Service functions handle common tasks
* ldServices: Service functions that interact with LaunchDarkly to fetch and update segment rules.
