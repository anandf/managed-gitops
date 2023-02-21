ARG BASE_IMAGE=registry.access.redhat.com/ubi8/ubi-minimal:8.5-218
################################################################################################
# Builder image
# Initial stage which pulls and prepares any required build dependencies for the whole monorepo
# Clones the whole monorepo and builds all the GitOps Service binaries using the Makefile
#
# Note: Make sure you use '.dockerignore' to avoid local copy of binaries (e.g. controller-gen)
################################################################################################
FROM golang:1.18 as builder

WORKDIR /workspace

COPY Makefile ./Makefile
COPY migrations ./migrations
COPY migrate ./migrate
COPY go.mod go.sum Makefile main.go ./

# Perform the build for all components
RUN go build -o ./db-migrate

################################################################################################
# GitOps Service image
# Provides both the 'gitops-service-backend' and 'gitops-service-cluster-agent' components
################################################################################################
FROM $BASE_IMAGE as db-migration

# Install the 'shadow-utils' which contains `adduser` and `groupadd` binaries
RUN microdnf install shadow-utils \
	&& groupadd --gid 65532 nonroot \
	&& adduser \
		--no-create-home \
		--no-user-group \
		--uid 65532 \
		--gid 65532 \
		nonroot

WORKDIR /

RUN mkdir -p /migrations

# Copy the database migration versions
COPY --from=builder workspace/migrations /migrations
COPY --from=builder workspace/db-migrate /db-migrate

# Run as non-root user
USER 65532:65532
