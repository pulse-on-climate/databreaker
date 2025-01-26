FROM amazonlinux:2

RUN yum update -y && \
    yum install -y wget tar gzip glibc glibc-devel && \
    amazon-linux-extras install -y python3.8

RUN python3.8 -m ensurepip && \
    pip3.8 install --no-cache-dir --upgrade pip && \
    pip3.8 install --no-cache-dir conda-pack

RUN rm -rf /opt/conda && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

ENV PATH=/opt/conda/bin:$PATH
ENV CONDA_ROOT_PREFIX=/opt/conda
ENV CONDA_PKGS_DIRS=/opt/conda/pkgs

COPY ./environment.yml .
RUN conda config --set channel_priority strict && \
    conda env update --name base --file ./environment.yml && \
    conda clean -afy