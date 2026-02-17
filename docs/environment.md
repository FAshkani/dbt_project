# Environments

## Environment Matrix
| Environment  | Fabric Workspace  | Warehouse | GitHub Environment | Notes |
|--------------|--------------------|-----------|---------------------|--------|
| development  | metcash_pdm_dev    | wh_dev    | development         | Daily development |
| test         | metcash_pdm_test   | wh_test   | test                | Pre-production |
| production   | metcash_pdm_prod   | wh_prod   | production          | Live system |

## Secrets & Config
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `PDM_WAREHOUSE_SQL_ENDPOINT_SHARED`

## Promotion Flow
feature → development → test → production
