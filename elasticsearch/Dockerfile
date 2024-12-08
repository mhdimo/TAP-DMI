FROM docker.elastic.co/elasticsearch/elasticsearch:8.16.1

# Copy custom configurations
COPY config/elasticsearch.yml /usr/share/elasticsearch/config/

USER root
RUN chown elasticsearch:elasticsearch /usr/share/elasticsearch/config/elasticsearch.yml
USER elasticsearch
