FROM docker.elastic.co/logstash/logstash:8.16.1

# Copy custom configurations
COPY logstash.conf /usr/share/logstash/config/

USER root
RUN chown logstash:logstash /usr/share/logstash/config/logstash.conf
USER logstash
