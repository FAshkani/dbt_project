# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Upgrade semantic-link to latest version to support SPN authentication
%pip install -U semantic-link

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import notebookutils as nb
from sempy import fabric

def get_item_id(workspace_id: str, item_name: str, item_type: str) -> str:
    df = fabric.list_items(type=item_type, workspace=workspace_id)
    df = df[df['Display Name'] == item_name]
    return df['Id'].values[0]

# Instantiate the variable library
var_lib = nb.variableLibrary.getLibrary("var_lib")

# Workspace names to resolve
workspace_names = [
    "cdm_workspace",
    "pdm_shared_workspace",
    "pdm_alm_workspace",
    "pdm_food_workspace",
    "inbound_workspace",
    "outbound_workspace",
    "sdm_shared_workspace",
    "sdm_alm_workspace",
    "sdm_food_workspace"
]

# Resolve workspace IDs
workspace_ids = {name: fabric.resolve_workspace_id(getattr(var_lib, name)) for name in workspace_names}

# Notebook definitions: (variable_name, workspace_key, item_name)
notebooks = [
    ("cdm_lakehouses_orchestrator_ddl_notebook_id", "cdm_workspace", "00_all_lakehouses_orchestrator_ddl_scripts"),
    ("inbound_lakehouses_orchestrator_ddl_notebook_id", "inbound_workspace", "00_all_lakehouses_orchestrator_ddl_scripts"),
    ("synapse_sync_notebook_id", "cdm_workspace", "Synapse Extract Daily Driver"),
    ("synapse_sync_retry_helper_notebook_id", "cdm_workspace", "Synapse Extract Retry Helper"),
    ("synapse_parquet_load_notebook_id", "cdm_workspace", "Synapse Load Parquet Driver"),
    ("cdm_master_notebook_id", "cdm_workspace", "master_transform_cdm_dbt_notebook"),
    ("pdm_shared_master_notebook_id", "pdm_shared_workspace", "master_pdm_dbt_notebook"),
    ("pdm_food_master_notebook_id", "pdm_food_workspace", "master_pdm_dbt_notebook"),
    ("pdm_alm_master_notebook_id", "pdm_alm_workspace", "master_pdm_dbt_notebook"),
    ("outbound_lakehouses_orchestrator_ddl_notebook_id", "outbound_workspace", "00_all_lakehouses_orchestrator_ddl_scripts"),
    ("extract_master_notebook_id", "outbound_workspace", "master_extract_dbt_notebook")
]

pipelines = [
    ("ingestion_pipeline_id", "inbound_workspace", "pl_extract_load")
]

var_dict = {}

# Get notebook IDs
for var_name, workspace_key, item_name in notebooks:
    var_dict[var_name] = get_item_id(workspace_id=workspace_ids[workspace_key], item_name=item_name, item_type="Notebook")

# Get pipeline IDs
for var_name, workspace_key, item_name in pipelines:
    var_dict[var_name] = get_item_id(workspace_id=workspace_ids[workspace_key], item_name=item_name, item_type="DataPipeline")

# Add workspace IDs to var_dict
for name, id_val in workspace_ids.items():
    var_dict[f"{name}_id"] = id_val

nb.notebook.exit(var_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # 
