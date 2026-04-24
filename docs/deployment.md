# Deployment Guide

## Local Development

All services start with a single command:

```bash
cp .env.example .env
# Add your OPENAI_API_KEY
make up
make seed
```

Services:

| Service | URL |
|---------|-----|
| React UI | http://localhost:3000 |
| FastAPI + OpenAPI docs | http://localhost:8000/docs |
| Neo4j Browser | http://localhost:7474 (neo4j / lineageiq_dev) |
| Flower (Celery monitor) | http://localhost:5555 |
| Kafka Connect (Debezium) | http://localhost:8083 |

## Registering the Debezium Connector

After `make up`:

```bash
make register-debezium
```

This registers the PostgreSQL CDC connector defined in `infra/debezium/postgres-connector.json`. Debezium will begin monitoring `lineageiq.schema_changes` for DDL events and emitting them to Kafka topic `lineageiq.lineageiq.schema_changes`.

The backend's Celery worker subscribes to this topic via the `process_cdc_event` task.

---

## AWS ECS Fargate (Production)

### Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform >= 1.6
- Docker with buildx
- An S3 bucket for Terraform state (update `backend "s3"` in `main.tf`)

### 1 · Build and push images

```bash
# Authenticate to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com

# Build and push
docker build -t lineageiq-backend ./backend
docker tag lineageiq-backend:latest <ecr-backend-url>:latest
docker push <ecr-backend-url>:latest

docker build -t lineageiq-frontend ./frontend
docker tag lineageiq-frontend:latest <ecr-frontend-url>:latest
docker push <ecr-frontend-url>:latest
```

### 2 · Apply Terraform

```bash
cd infra/terraform

terraform init
terraform plan -var="openai_api_key=sk-..." -var="neo4j_password=yourpassword"
terraform apply
```

This provisions:
- VPC with public/private subnets across 2 AZs
- ECS Fargate cluster with backend and worker services
- ElastiCache Redis (cache.t3.micro)
- ECR repositories for backend and frontend
- IAM roles with least-privilege Secrets Manager access
- CloudWatch log groups for all services
- Secrets Manager secrets for OpenAI key and Neo4j password

### 3 · Neo4j

For production, deploy Neo4j Enterprise on EC2 or use [Neo4j AuraDB](https://neo4j.com/cloud/platform/aura-graph-database/) (managed). The Terraform module does not provision Neo4j — it's intentionally left external since Neo4j sizing depends on your graph scale.

Update `NEO4J_URI` in the ECS task definition with the production Neo4j endpoint.

### 4 · Environment Variables (Production)

All secrets are stored in AWS Secrets Manager under `lineageiq/*`. The ECS task definition pulls them via the `secrets` block in the container definition.

Non-secret configuration is passed as `environment` variables:

```
ENVIRONMENT=production
REDIS_URL=redis://<elasticache-endpoint>:6379/0
NEO4J_URI=bolt://<neo4j-host>:7687
KAFKA_BOOTSTRAP_SERVERS=<msk-bootstrap>:9092
SCHEMA_REGISTRY_URL=https://<schema-registry>:8081
```

---

## Scaling Celery Workers

Worker scaling is independent per queue:

```bash
# Scale Spark workers (CPU-heavy) up
docker compose up -d --scale worker=4

# In ECS, create separate task definitions per queue:
# - lineageiq-worker-spark: concurrency=2, cpu=2048
# - lineageiq-worker-io:    concurrency=8, cpu=512 (dbt + kafka)
# - lineageiq-worker-sql:   concurrency=4, cpu=1024
```

---

## Health Checks

```bash
# Liveness
curl http://localhost:8000/healthz
# {"status": "ok"}

# Readiness (checks Neo4j)
curl http://localhost:8000/readyz
# {"status": "ready", "neo4j": "connected"}

# Graph statistics
curl http://localhost:8000/api/v1/stats
```

---

## Monitoring

- **Prometheus metrics** are exposed at `/metrics` (via `prometheus-fastapi-instrumentator`)
- **Celery** task monitoring via Flower at `:5555`
- **Neo4j** metrics via the built-in Neo4j admin interface at `:7474`
- **Structured logs** via `structlog` — all log lines are JSON, ready for CloudWatch Logs Insights
