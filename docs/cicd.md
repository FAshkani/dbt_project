# CI/CD Pipeline

Documentation for GitHub Actions workflows that build, test, and deploy the platform.

## Workflow Steps
1. Checkout repository
2. Azure OIDC login
3. Python setup
4. Install dependencies
5. dbt deps/compile/docs generate
6. Prepare site
7. Commit versioned docs
8. Publish/dbt sync

## Secrets
Ensure job attaches to the correct GitHub Environment.
