# ticketmaster2kafka

Create an env file with the following parameters:
- KAFKA_BOOTSTRAP
- KAFKA_USER
- KAFKA_PASSWORD

The Strimzi CA cert must live on the host at `/etc/viewlive/certs/strimzi-ca.crt`
and is mounted read-only into the container. Override with `KAFKA_CA_LOCATION`
if you need a different in-container path.

Use that env file in the following:
```
docker build -t ticketmaster2kafka:0.0 .
docker run \
  --env-file path/to/1.env \
  -v /etc/viewlive/certs:/etc/viewlive/certs:ro \
  ticketmaster2kafka:0.0
```

### CI/CD

Drone (`.drone.yml`) runs on every push to `main`: it builds the image, pushes
`docker.mogi.io/ticketmaster2kafka:<commit-sha>`, then bumps the image tag in
`2kafka/ticketmaster/deployment.yaml` of `k8s-manifests-backend` to deploy.

