## End to End Domino

This repo contains examples of how to use complex parts of Domino and end to end examples of how to use Domino to enable an MLOps process

Domino MLOps process involves the following steps:

1. Configure Domino Environments and Hardware tiers - Environments are Docker images ane HW Tiers define the compute used to run those images.
2. Create a Domino Project and attach it to an external code repository (Domino supports an internal code repository as well known as Domino File System)
3. A Domino user creates their own token to access the external repository and attaches to their user profile
4. The Domino user then connects to their project code base with the token they created in step 3
5. The Domino user starts a workspace to interactively experiment on Models. These experiments are stored in Domino Experiment Manager and Model Registry
6. Once satisfied the Domino user will package their code in proper packages and scripts so they can be executed from the command line. They will test if the scripts can be invoked from a Domino Job.
7. Finally the Domino Admin will create Domino Service Accounts and OAuth tokens for these accounts with requisite permissions to run the jobs and share those tokens with the Operations team which will then invoke these jobs using these tokens from their workflow pipelines

   
