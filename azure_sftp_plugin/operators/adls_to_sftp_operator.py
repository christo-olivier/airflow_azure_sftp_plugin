import fnmatch
import os
import tempfile
from typing import List
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure_sftp_plugin.hooks.adls_gen2_hook import ADLSGen2Hook
from airflow.contrib.hooks.ssh_hook import SSHHook
from azure.storage.filedatalake._generated.models._models_py3 import (
    StorageErrorException,
)


class ADLSToSFTPOperator(BaseOperator):
    """
    This operator is used to upload files in a Azure Data Lake Storage Gen 2 folder
    to an SFTP server location.

    :param sftp_conn_id: Connection Id in Airflow for the SFTP server .
    :param sftp_folder_path: Folder path on SFTP server where files are to be 
                             uploaded to.
    :param adls_container: The ADLS container that is to be used.
    :param adls_folder_path: The ADLS folder path in which the source files are
                             located.
    :param source_object: Name of file in ADLS folder to be uploaded or the 
                          name prefixed with wildcard to upload all files with 
                          that prefix. E.g. *.txt to upload all files with a 
                          `txt` extension.
    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection.
                                    storage_account_name and storage_account_key 
                                    should be in the `login` and `password` 
                                    fields of the azure data lake connection.
    :param reload_all: Flag to specify if files that already exist in ADLS 
                       folder should be re-downloaded.
    """

    template_fields = [
        "sftp_folder_path",
        "adls_container",
        "adls_folder_path",
        "source_object",
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
        source_object: str = None,
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
        self.source_object = source_object
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

        # Get list of files in ADLS folder
        source_files = [
            os.path.split(file)[1]  # get only the file portion of the path
            for file in self._get_adls_files()
            if fnmatch.fnmatch(os.path.split(file)[1], self.source_object)
        ]
        self.log.info(f"Source Files: `{source_files}`")

        # Get list of files in sftp_path
        try:
            self.log.info(
                f"Getting list of files in sftp_path: `{self.sftp_folder_path}`"
            )
            sftp_files = sftp_client.listdir(self.sftp_folder_path)
        except IOError as e:
            self.log.error(
                f"The folder `{self.sftp_folder_path}` does not exist on the sftp server."
            )
            raise e

        # determine the files to be processed. If all files are to be reloaded
        # then process all filesin the ADLS folder that match the `source object`.
        # If all files are not to be reloaded then only process files for which
        # the file name does not currently exist in the sftp folder
        if self.reload_all:
            files_to_process = source_files
            self.log.info(f"Files to process: `{files_to_process}`")
        else:
            self.log.info(f"Existing files in sftp folder: `{sftp_files}`")
            files_to_process = set(source_files) - set(sftp_files)
            self.log.info(f"Files to process: `{files_to_process}`")

        # create temporary folder and process files
        with tempfile.TemporaryDirectory() as temp_folder:
            for file in files_to_process:
                temp_path = os.path.join(temp_folder, file)
                adls_object = os.path.join(self.adls_folder_path, file)
                sftp_object = os.path.join(self.sftp_folder_path, file)

                self.log.info(f"Processing file: `{adls_object}`")
                self._adls_hook.download_file(
                    local_path=temp_path, remote_path=adls_object, overwrite=True
                )
                sftp_client.put(localpath=temp_path, remotepath=sftp_object)
                os.remove(temp_path)

        # Close ADLS Connection
        self._adls_hook.connection.close()
