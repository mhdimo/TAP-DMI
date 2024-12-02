#!/bin/bash

# Wait for Elasticsearch
until curl -s http://elasticsearch:9200 > /dev/null; do
    echo 'Waiting for Elasticsearch...'
    sleep 5
done

# Wait for Kibana to be ready
until curl -s http://localhost:5601/api/status | grep -q '"overall":{"level":"available"'; do
    echo 'Waiting for Kibana...'
    sleep 5
done

# Create index pattern
curl -X POST "localhost:5601/api/saved_objects/index-pattern/chat_messages" \
    -H 'kbn-xsrf: true' \
    -H 'Content-Type: application/json' \
    -d '{
      "attributes": {
        "title": "chat_messages*",
        "timeFieldName": "timestamp"
      }
    }'

# Import dashboards
for dashboard in /usr/share/kibana/dashboards/*.json; do
    curl -X POST "localhost:5601/api/saved_objects/_import" \
        -H "kbn-xsrf: true" \
        --form file=@$dashboard
done

# Start Kibana
/usr/local/bin/kibana