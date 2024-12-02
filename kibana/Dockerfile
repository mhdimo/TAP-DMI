FROM docker.elastic.co/kibana/kibana:7.9.3

# Copy configuration
COPY config/kibana.yml /usr/share/kibana/config/
COPY dashboards /usr/share/kibana/dashboards/

# Add setup script
COPY setup.sh /usr/share/kibana/
RUN chmod +x /usr/share/kibana/setup.sh

USER kibana

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:5601 || exit 1

ENTRYPOINT ["/usr/share/kibana/setup.sh"]