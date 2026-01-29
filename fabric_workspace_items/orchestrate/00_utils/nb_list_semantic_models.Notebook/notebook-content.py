# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

workspace_id="EDAA_SDM_SHARED_MAIN_DEV_001"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import notebookutils as nb
from sempy import fabric

dataset_list = fabric.list_datasets(workspace=workspace_id)
if len(dataset_list) > 0:
    output_value = dataset_list['Dataset ID'].tolist()
else:
    output_value = [0]
    
    
nb.notebook.exit(output_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
