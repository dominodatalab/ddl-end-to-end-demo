# End to End Domino

This repo contains examples of how to use complex parts of Domino and end to end examples of how to use Domino to enable an MLOps process


## First the basics

Domino has extensive courses available online via the [Dominio University](https://university.domino.ai/). The following is a useful
pre-requisite:
1. [Training to be a Domino User](https://university.domino.ai/page/practitioner-training)
2. [Training to be a Domino Admin](https://university.domino.ai/page/domino-administrator)
3. [Customer Tech hours](https://university.domino.ai/page/customer-tech-hours) - We run this perodically sharing technical design on interesting challenges we have address with customers
4. [Domino Blueprints](https://domino.ai/resources/blueprints) - This is a blog created by members of the Professional Services tacking some of the most challenging integrations with Domino.

   
## What this repo provides

This repo is to get into the most advanced use-cases with Domino. Once you have acquired a basic knowledge of how to use various aspects of
Domino and are looking to use Domino for production use-cases, this repo aims to provide examples of challenges you will face and how to 
address them. We intend to keep this repo an evolving one.

Domino MLOps process involves the following steps:

1. Configure Domino Environments and Hardware tiers - Environments are Docker images ane HW Tiers define the compute used to run those images.
2. Create a Domino Project and attach it to an external code repository (Domino supports an internal code repository as well known as Domino File System)
3. A Domino user creates their own token to access the external repository and attaches to their user profile
4. The Domino user then connects to their project code base with the token they created in step 3
5. The Domino user starts a workspace to interactively experiment on Models. These experiments are stored in Domino Experiment Manager and Model Registry
6. Once satisfied the Domino user will package their code in proper packages and scripts so they can be executed from the command line. They will test if the scripts can be invoked from a Domino Job.
7. Finally the Domino Admin will create Domino Service Accounts and OAuth tokens for these accounts with requisite permissions to run the jobs and share those tokens with the Operations team which will then invoke these jobs using these tokens from their workflow pipelines


Some of the topics covered are-

1. Hands on example of how to use [MLflow](mock_mlflow//README.md). A basic mlflow client [notebook](notebooks/basic_mlflow_client.ipynb) to demonstrate how to create an ML experiment using XGBoost, register it to Model Registry and then deploy it as a Domino Endpoint to be accessed via client programs.
2. Basic [Spark based notebook](notebooks/basic_spark_job.ipynb) with the ability to scale up and down. This demonstrates how you can use IRSA to connect Spark to your S3 bucket. 
3. Basic [Ray based notebook](notebooks/basic_ray_job.ipynb) with the ability to scale up and down
4. Example [notebook](notebooks/cluster_scaler_client.ipynb) demonstrating how to scale Spark/Ray/Dask clusters with an API endpoint
5. Example [notebook](service_accounts/service_accounts_mgmt.ipynb) to create Service Accounts, configure them for use with Git backed projects and use them to execute jobs in Domino
6. Example [notebook](mlflow_external_access/access_mlflow_externally.ipynb) demostrating to accesss Domino MLflow externally. This capability is crucial when downloading models using your CI/CD pipelines

For Domino Flows follow this [repo](https://github.com/dominodatalab/domino-MLops-flows).
