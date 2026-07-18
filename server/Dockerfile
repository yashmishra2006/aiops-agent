FROM node:22-alpine
WORKDIR /app
COPY aiops-server.js .
ENV AIOPS_DB=/data/aiops.db
VOLUME /data
EXPOSE 8480
CMD ["node", "--experimental-sqlite", "aiops-server.js"]
