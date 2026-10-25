ARG WEB_IMAGE=nginxinc/nginx-unprivileged@sha256:7377697a821c131a924a7105fafbe7414db4e9fcc77a6f08f776f33f141ec3f8
FROM ${WEB_IMAGE}
ARG REVISION
LABEL org.opencontainers.image.source="https://github.com/joejaeyoung/jigeumini" org.opencontainers.image.revision="${REVISION}"
COPY dist/ /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/conf.d/default.conf
USER 101:101
ENTRYPOINT ["nginx","-g","daemon off;"]
