.PHONY: help build up down logs shell migrate makemigrations createsuperuser collectstatic build-prod up-prod down-prod ghcr-build ghcr-push ghcr-buildx-setup ghcr-buildx-push up-prod-ghcr reset-prod-local

GHCR_IMAGE ?= ghcr.io/tulonbaar/dock-app.oracle-log-exporter
GHCR_TAG ?= 1.0.0
GHCR_PLATFORMS ?= linux/amd64,linux/arm64
GHCR_BUILDER ?= tulon-multiarch

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Development ──────────────────────────────────────

build: ## Build (dev)
	docker compose build

up: ## Up (dev)
	docker compose up

up-d: ## Up in detached mode (dev)
	docker compose up -d

down: ## Down (dev)
	docker compose down

logs: ## Logs (dev)
	docker compose logs -f

ghcr-build: ## Build GHCR image
	docker build -t $(GHCR_IMAGE):$(GHCR_TAG) .

ghcr-push: ## Push GHCR image
	docker push $(GHCR_IMAGE):$(GHCR_TAG)

ghcr-buildx-setup: ## Setup and prepare buildx builder for multi-arch images
	docker buildx inspect $(GHCR_BUILDER) >/dev/null 2>&1 || docker buildx create --name $(GHCR_BUILDER) --use
	docker buildx use $(GHCR_BUILDER)
	docker buildx inspect --bootstrap

ghcr-buildx-push: ## Build and push multi-arch image to GHCR
	docker buildx inspect $(GHCR_BUILDER) >/dev/null 2>&1 || docker buildx create --name $(GHCR_BUILDER) --use
	docker buildx use $(GHCR_BUILDER)
	docker buildx build --platform $(GHCR_PLATFORMS) -t $(GHCR_IMAGE):$(GHCR_TAG) --push .