FROM quay.io/astronomer/astro-runtime:12.4.0

USER root

# Required for some ML/DS dependencies
RUN apt-get update -y
RUN apt-get install libgomp1 -y
RUN apt-get install -y git

USER astro