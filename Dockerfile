FROM ubuntu:jammy-20231004

# install protoc
RUN apt update \
	&& apt -y upgrade \
	&& apt install -y --no-install-recommends \
	&& apt install -y python3-pip


#Make shure to build latest wheels before runing docker build
COPY ./dist/*.whl /e-soh-api/
COPY ./pygeoapi-config.yml /e-soh-api/
COPY ./pygeoapi-openapi-config.yml /e-soh-api/
RUN pip install /e-soh-api/pygeoapi*.whl && \
	pip install /e-soh-api/esoh_api*.whl
RUN pip install pydantic=~2.3

WORKDIR /e-soh-api



CMD ["/usr/local/bin/pygeoapi", "serve", "--flask"]
