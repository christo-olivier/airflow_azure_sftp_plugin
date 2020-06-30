from airflow.plugins_manager import AirflowPlugin
from azure_sftp_plugin.hooks.adls_gen2_hook import ADLSGen2Hook
from azure_sftp_plugin.operators.sftp_to_adls_operator import SFTPToADLSOperator
from azure_sftp_plugin.operators.adls_to_sftp_operator import ADLSToSFTPOperator
from azure_sftp_plugin.operators.sftp_delete_file_operator import SFTPDeleteFileOperator


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "azure_sftp_plugin"
    hooks = [ADLSGen2Hook]
    operators = [ADLSToSFTPOperator, SFTPToADLSOperator, SFTPDeleteFileOperator]
