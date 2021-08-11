ARG BASE_IMAGE_TAG=latest
FROM tonytunde2012/feature_ops:tagname:$BASE_IMAGE_TAG
COPY requirements.txt .
RUN python3 -m pip install -r \
    requirements.txt --quiet --no-cache-dir \
    && rm -f requirements.txt
COPY data_extractor.py /pipelines/component/src/data_extractor.py

ENTRYPOINT python3 /pipelines/component/src/data_extractor.py

# 