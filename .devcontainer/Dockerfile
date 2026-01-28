FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    openssh-client \
    bash \
    vim \
    wget \
    apt-utils \
    sudo \
    clangd \
    net-tools \
 && rm -rf /var/lib/apt/lists/*

USER root

