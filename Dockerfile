# syntax=docker/dockerfile:1
FROM conda/miniconda3 as buildpy
RUN --mount=type=cache,target=/usr/local/pkgs conda install mamba conda-pack -n base -c conda-forge

# copy environment.yml and build conda environment w/ skylark
COPY environment.yml .
RUN --mount=type=cache,target=/usr/local/pkgs mamba env create -p /env --file environment.yml
RUN conda clean -afy

# pack
RUN conda-pack -p /env -o /tmp/env.tar --exclude /env/python3.9 && \
  mkdir /env_out && cd /env_out && tar xf /tmp/env.tar && \
  rm /tmp/env.tar && rm -rf /env/lib/python* /env/lib/pip*
RUN /env_out/bin/conda-unpack

# copy installed python package to deployment image
FROM ubuntu:18.04 as runtime
COPY --from=buildpy /env_out /env
ENV PATH /env/bin:$PATH

# todo enable BBR by default

WORKDIR /pkg
COPY . .
RUN pip install -e .