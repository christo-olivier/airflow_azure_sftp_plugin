import fnmatch
import os
import tempfile
from typing import List
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ssh_hook import SSHHook
from azure_sftp_plugin.hooks.adls_gen2_hook import ADLSGen2Hook
from azure.storage.filedatalake._generated.models._models_py3 import (
    StorageErrorException,
)


class SFTPToADLSOperator(BaseOperator):
    """
    This operator is used to download files from an SFTP server to an Azure Data
    Lake Storage Gen 2 location.

    :param sftp_conn_id: Connection Id in Airflow for the SFTP server.
    :param sftp_folder_path: Folder path on SFTP server where files are to be
                             downloaded from.
    :param sftp_filename: Name of the file on SFTP server to download, or unix
                          wildcard characters to
                          specify specific or all files. If left empty all files
                          in the folder will be downloaded.
    :param adls_container: The ADLS container that is to be used.
    :param adls_folder_path: The ADLS folder path in which the files should be placed.
    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection.
                                    storage_account_name and storage_account_key 
                                    should be in the `login` and `password` 
                                    fields of the azure data lake connection.
    :param reload_all: Flag to specify if files that already exist in ADLS 
                       folder should be re-downloaded.
    """

    template_fields = [
        "sftp_folder_path",
        "sftp_filename",
        "adls_container",
        "adls_folder_path",
    ]
    template_ext = []
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        sftp_conn_id: str,
        sftp_folder_path: str,
        adls_container: str,
        adls_folder_path: str,
        sftp_filename: str = None,
        azure_data_lake_conn_id: str = "azure_data_lake_default",
        reload_all: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_folder_path = sftp_folder_path
        self.adls_container = adls_container
        self.adls_folder_path = adls_folder_path
        self.sftp_filename = sftp_filename if sftp_filename else "*"
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.reload_all = reload_all
        self._adls_hook = None

    def _get_adls_files(self) -> List[str]:
        """
        Return the files that exist in the ADLS folder. If Storage Exception
        is raised then return a blank list.

        :return: List[str]
        """
        try:
            self.log.info(
                f"Getting list of files in adls_folder_path: `{self.adls_folder_path}`"
            )
            adls_files = [
                file
                for file in self._adls_hook.list_files(
                    path=self.adls_folder_path, recursive=False
                )
            ]
        except StorageErrorException:
            self.log.info(f"`{self.adls_folder_path}` does not exist.")
            adls_files = []

        return adls_files

    def execute(self, context):
        if not self._adls_hook:
            self._adls_hook = ADLSGen2Hook(
                container=self.adls_container,
                azure_data_lake_conn_id=self.azure_data_lake_conn_id,
            )
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        # Get list of files in sftp_path
        self.log.info(f"Getting list of files in sftp_path: `{self.sftp_folder_path}`")
        path_content = sftp_client.listdir(self.sftp_folder_path)
        files = [
            file for file in path_content if fnmatch.fnmatch(file, self.sftp_filename)
        ]

        # Get files that already exist in the ADLS folder
        existing_files = self._get_adls_files()

        # Determine the files to be processed. If all files are to be reloaded then process all files
        # in the sftp file list. If all files are not to be reloaded then only process files for
        # which the file name does not currently exist in the ADLS folder
        if self.reload_all:
            files_to_process = files
        else:
            existing_set = {os.path.split(filename)[1] for filename in existing_files}
            files_to_process = set(files) - existing_set
            self.log.info(f"Existing files in ADLS: `{existing_set}`")
            self.log.info(f"Files to process: `{files_to_process}`")

        # create temporary folder and process files
        with tempfile.TemporaryDirectory() as temp_folder:
            for file in files_to_process:
                temp_path = os.path.join(temp_folder, file)
                adls_object = os.path.join(self.adls_folder_path, file)
                sftp_object = os.path.join(self.sftp_folder_path, file)

                self.log.info(f"Processing: `{sftp_object}`")
                try:
                    sftp_client.get(sftp_object, temp_path)
                    self._adls_hook.upload_file(
                        local_path=temp_path,
                        remote_path=adls_object,
                        overwrite=self.reload_all,
                    )
                    os.remove(temp_path)
                except IOError:
                    self.log.info(f"Skipping directory `{sftp_object}`.")

        # Close ADLS Connection
        self._adls_hook.connection.close()
