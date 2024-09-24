# prefect-container-poc
Proof of concept for running a legacy Prefect v1 flow in a container without the Prefect Cloud infrastructure.

This will allow the IDP team to get the Requirements Course List (CMS-3) Prefect flow out of the ET Prefect Cloud infrastructure so the product can be retired.

This approach is a lift-and-shift approach that will allow the Prefect code to run as-is, but will require some backing infrastructure to run.

## Steps to Run
1. Set `db_user` and `db_host` to actual values in the `with FLow(...):` block.
2. Build the image: `docker build -t prefect-v1-docker-image .`
3. Start the container, which will run the flow on start: `docker run prefect-v1-docker-image`
4. Container should start and the flow should execute immediately.

## Further Work
* Integrate with ECS to provision and destroy the container to run the flow.
  * Lambda is not a good candidate for this as the flow takes 11+ hours to complete.
* Container executing the flow will need access to ODS.  This will likely require that the container be running within a VPC with a VPC endpoint to ODS configured.
* Database credentials will need to be stored in Vault and retrieved by or provided to the container.