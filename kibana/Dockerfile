FROM docker.elastic.co/kibana/kibana:8.16.1

# Copy custom configurations
COPY config/kibana.yml /usr/share/kibana/config/

USER root
RUN chown kibana:kibana /usr/share/kibana/config/kibana.yml
USER kibana
