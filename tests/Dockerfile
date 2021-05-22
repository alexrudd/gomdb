FROM postgres:13.2-alpine

ENV MESSAGEDB_VER=v1.2.6

# Fetch Message DB
ADD https://github.com/message-db/message-db/archive/refs/tags/${MESSAGEDB_VER}.tar.gz message-db.tar.gz
RUN mkdir -p /opt/message-db; tar -xvf message-db.tar.gz -C /opt/message-db --strip-components=1

# Add Message DB initialization script
RUN echo "#!/bin/bash" > /docker-entrypoint-initdb.d/init-message-db.sh
RUN echo "cd /opt/message-db/database/; ./install.sh" >> /docker-entrypoint-initdb.d/init-message-db.sh
