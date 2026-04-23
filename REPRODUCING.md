This [Code Ocean](https://codeocean.com) Compute Capsule will allow you to run and reproduce the results of [Exaspim Launch EMR Fusion](https://codeocean.allenneuraldynamics.org/capsule/4946190/tree) on your local machine<sup>1</sup>. Follow the instructions below, or consult [our knowledge base](https://docs.codeocean.com/user-guide/compute-capsule-basics/managing-capsules/exporting-capsules-to-your-local-machine) for more information. Don't hesitate to reach out to [Support](mailto:support@codeocean.com) if you have any questions.

<sup>1</sup> You may need access to additional hardware and/or software licenses.

# Prerequisites

- [Docker Community Edition (CE)](https://www.docker.com/community-edition)

# Instructions

## Log in to the Docker registry

In your terminal, execute the following command, providing your password or API key when prompted for it:
```shell
docker login -u cameron.arshadi@alleninstitute.org registry.codeocean.allenneuraldynamics.org
```

## Run the Capsule to reproduce the results

In your terminal, navigate to the folder where you've extracted the Capsule and execute the following command, adjusting parameters as needed:
```shell
docker run --platform linux/amd64 --rm \
  --workdir /code \
  --volume "$PWD/code":/code \
  --volume "$PWD/data":/data \
  --volume "$PWD/results":/results \
  --env AWS_ACCESS_KEY_ID=value \
  --env AWS_SECRET_ACCESS_KEY=value \
  --env AWS_DEFAULT_REGION=value \
  registry.codeocean.allenneuraldynamics.org/capsule/dc2d4fdd-3129-4408-8683-177ab5609b56 \
  bash run '' '' '' AVG_BLEND
```

As secrets are required, replace all `value` occurances with your personal credentials prior to your run.
