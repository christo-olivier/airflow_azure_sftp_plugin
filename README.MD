# sFTP to Azure Data Lake Storage Gen 2 Plugin

This plugin provides operators and a hook allowing you to sync files between an sFTP server and an Azure Data Lake Storage Gen 2 folder.

## Hooks

The ADLSGen2Hook allows the following actions against an ADLS container:

1. Checking if a file exists
1. Deletion of a file
1. Downloading files
1. Listing of files in a folder
1. Uploading of files

The hook can also function as a context manager, however the operators here do not currently use it in that way.

## Operators

There are currently 3 operators created as part of the plugin:

1. ADLSToSFTPOperator
1. SFTPToADLSOperator
1. SFTPDeleteFileOperator

## Installation

In order to make use of the plugin copy the `azure_sftp_plugin` package in the root of the repository into
the `$AIRFLOW_HOME/plugins` folder. If the `$AIRFLOW_HOME/plugins` folder does not exist, then simply create it.

Once you have completed your Airflow plugins folder should look like this

```
$AIRFLOW_HOME/plugins/azure_sftp_plugin
$AIRFLOW_HOME/plugins/azure_sftp_plugin/__init__.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/hooks/__init__.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/hooks/adls_gen2_hook.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/operators/__init__.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/operators/adls_to_sftp_operator.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/operators/sftp_to_adls_operator.py
$AIRFLOW_HOME/plugins/azure_sftp_plugin/operators/sftp_delete_file_operator.py
```

In order to use the hook and operator use the following imports:

```Python
from airflow.hooks.azure_sftp_plugin import ADLSGen2Hook
from airflow.operators.gcp_sftp_plugin import ADLSToSFTPOperator, SFTPToADLSOperator, SFTPDeleteFileOperator
```
