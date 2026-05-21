FROM confluentinc/cp-kafka:7.6.1

# Bake in the log4j config fix so deployments don't need to carry it as an env var.
# connect-mirror-maker resolves the log4j config path relative to its own location
# (/usr/bin/../config/ = /usr/config/) which doesn't exist in this image.
# Setting this env var at the image level points it at the actual config file.
ENV KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/etc/kafka/connect-log4j.properties"
