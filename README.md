# TalentIQ

RAG-powered resume search POC. Upload resumes, extract structured data via LLM, embed and store vectors, then search with natural language.

## Architecture

- **FastAPI** — single API service for ingestion and search
- **Ollama** — llama3.2:8b for extraction/reranking, nomic-embed-text for embeddings
- **Postgres + pgvector** — vector store and metadata
- **MinIO** — S3-compatible object storage for raw/processed resume files
- **Plain HTML/JS** — single-page UI

## Prerequisites

- Docker & Docker Compose (local dev)
- `kubectl` configured for your cluster (k8s deploy)
- Ollama running with required models:

```bash
ollama list
# Should show: llama3.2:8b, nomic-embed-text
# If not:
ollama pull llama3.2:8b
ollama pull nomic-embed-text
```

## Local Dev

```bash
docker-compose up --build
```

- UI: http://localhost:8000
- API: http://localhost:8000/api
- MinIO Console: http://localhost:9001 (talentiq / talentiq123)

Ollama must be running on your host machine (port 11434).

## Kubernetes Deploy

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/postgres.yaml

# Build and push the API image to your registry, then update k8s/api.yaml image field
kubectl apply -f k8s/api.yaml
```

Access at: https://talentiq.sudothai.com

## Example Search Queries

- "senior Python developer with AWS experience"
- "data scientist with machine learning and NLP skills"
- "frontend engineer experienced with React and TypeScript"
- "DevOps engineer with Kubernetes and CI/CD pipelines"
- "full stack developer with 5+ years experience"
