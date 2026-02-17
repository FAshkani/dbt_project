# Troubleshooting

## Common Issues
### OIDC: Missing client-id/tenant-id
Cause: Environment not attached.
Fix: Set job `environment:` properly.

### dbt: Missing Profiles
Cause: Wrong working directory.
Fix: Set `working-directory:` during dbt steps.

### dbt docs: Missing env vars
Cause: dbt uses env vars during parsing.
Fix: Ensure env vars are passed to all dbt steps.
