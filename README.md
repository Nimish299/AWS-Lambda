#Domain Update Pipeline


This pipeline automates the process of updating email domains in LaunchDarkly (LD) based on data fetched from AWS S3. The pipeline retrieves domain lists stored in S3, processes them, and updates LD with the latest data. It includes functions for fetching, filtering, and updating domains through API calls.

---

**Pipeline Overview**
##The pipeline follows this basic flow:



                                          ------ s3services -----> S3
                                          |
      lambda/handler.handler -----> processLdRequest
                                          |
                                           ------ ldservices -----> LD

* Handler: The entry point (Lambda function) that triggers the pipeline.
* processLdRequest: The main workflow that coordinates fetching data from S3 and updating LD.
* s3services: Service functions that interact with AWS S3 to list and retrieve domain data.
* ldservices: Service functions that interact with LaunchDarkly to fetch and update segment rules.
