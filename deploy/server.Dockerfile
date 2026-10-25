ARG JAVA_IMAGE=eclipse-temurin@sha256:affcab5356e1270ac35d2361ee83135af2f03e8d580eca78a6d5d6fdbf048db0
FROM ${JAVA_IMAGE}
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/joejaeyoung/jigeumini" org.opencontainers.image.revision="${REVISION}"
WORKDIR /app
COPY classes/ /app/classes/
COPY lib/ /app/lib/
USER 10001:10001
ENV APP_REVISION=${REVISION}
ENTRYPOINT ["java","-Xms64m","-Xmx384m","-cp","/app/classes:/app/lib/*","io.chatpulse.poc.PocApplication"]
