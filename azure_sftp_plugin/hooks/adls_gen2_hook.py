from pathlib import Path
from typing import Generator
from airflow.hooks.base_hook import BaseHook
from azure.storage.filedatalake import FileSystemClient
from azure.storage.filedatalake._generated.models._models_py3 import (
    StorageErrorException,
)


class ADLSGen2Hook(BaseHook):
    """
    Hook to interact with the Azure Data Lake Gen 2 storage service.

    :param container: Name of the ADLS Gen 2 container to be used.
    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection.
                                    storage_account_name and storage_account_key 
                                    should be in the `login` and `password` 
                                    fields of the azure data lake connection.
    """

    def __init__(
        self, container: str, azure_data_lake_conn_id: str = "azure_data_lake_default"
    ):
        self.container = container
        self.conn_id = azure_data_lake_conn_id
        self.connection = self.get_conn()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.connection.close()

    def get_conn(self) -> FileSystemClient:
        """
        Return an Azure Data Lake Service Client object.
        :return: FileSystemClient
        """
        conn = self.get_connection(self.conn_id)
        file_system_client = FileSystemClient(
            account_url=f"https://{conn.login}.dfs.core.windows.net",
            file_system_name=self.container,
            credential=conn.password,
        )
        return file_system_client

    def check_for_file(self, file_path: str, recursive: bool = True) -> bool:
        """
        Check if a file exists in on Azure Data Lake.

        :param file_path: File path on Azure Data Lake.
        :param recursive: Specify if the path should be traversed recursively.
        :return: bool
        """
        try:
            return file_path in self.list_files(path=file_path, recursive=recursive)
        except StorageErrorException:
            return False

    def list_files(
        self, path: str, recursive: bool = True
    ) -> Generator[str, None, None]:
        """
        List files in an Azure Data Lake Store Gen 2 container.

        :param path: The path in the container that needs to be listed.
        :param recursive: Specify if the path should be traversed recursively.
        :return: Generator
        """
        # Create generator of path names instead of path objects and yield
        # the value from it.
        yield from (
            path.name
            for path in self.connection.get_paths(path=path, recursive=recursive)
        )

    def delete_file(self, remote_path: str) -> None:
        """
        Delete a file from ADLS.

        :param remote_path: Remote path where the file is located on ADLS.
        :return: None
        """
        self.connection.delete_file(file=remote_path)

    def download_file(
        self, local_path: str, remote_path: str, overwrite: bool = True
    ) -> None:
        """
        Download a file from ADLS to the local path.

        :param local_path: Local path where the file is to be downloaded to.
        :param remote_path: Remote path where the file is located on ADLS.
        :param overwrite: Should the file be overwritten if it already exists in
                          the local path.
        :return: None
        """
        # Check if the local file exists and if overwrite is `True` otherwise
        # raise an exception
        if not overwrite:
            path = Path(local_path)
            if path.exists():
                msg = f"`{local_path}` already exists and overwrite is set to False."
                raise FileExistsError(msg)

        # Check the file exists on ADLS
        if not self.check_for_file(file_path=remote_path, recursive=False):
            raise FileNotFoundError(f"`{remote_path}` does not exist.")

        with open(local_path, "wb") as fout:
            file_client = self.connection.get_file_client(file_path=remote_path)
            download = file_client.download_file()
            download.readinto(fout)

    def upload_file(
        self, local_path: str, remote_path: str, overwrite: bool = True
    ) -> None:
        """
        Upload a file from the local path to the remote path on Azure Data Lake.

        :param local_path: Local path where the file is located.
        :param remote_path: Remote path where the file is to be uploaded to.
        :param overwrite: Should the file be overwritten if it already exists in
                          the remote path.
        :return: None
        """
        # If `overwrite` is not True then check to see if the file exist and
        # raise an excption if it does.
        # NB this is required as the current Microsoft SDK's does not provide
        # an elegant way of uploading files that dont exist on ADLS yet. It
        # raises a generic error which is too broad. The only way to deal
        # with this is to manually check if a file already exists and raise an
        # exception if the user specifies `overwrite` to be False.
        if not overwrite:
            if self.check_for_file(file_path=remote_path, recursive=False):
                raise FileExistsError(f"`{remote_path}` already exists on ADLS.")

        # As above, overwrite is set to True as otherwise new files will fail
        # to upload to ADLS. The previous check will make sure no existing files
        # are overwritten if the user does not want this to happen.
        with open(local_path, "rb") as fin:
            file_client = self.connection.get_file_client(file_path=remote_path)
            file_client.upload_data(fin.read(), overwrite=True)
