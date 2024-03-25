FROM python:3.11-slim
RUN mkdir -p /home/polaris_mdms && addgroup polaris && useradd -d /home/polaris_mdms -g polaris Sumit && chown Sumit:polaris /home/polaris_mdms
RUN apt-get update && apt-get install -y curl
USER Sumit
WORKDIR /home/polaris_mdms
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH=/home/polaris_mdms/.local/bin:$PATH
RUN poetry config virtualenvs.in-project true
