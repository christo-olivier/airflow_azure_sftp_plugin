import fnmatch
import os
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.ssh_hook import SSHHook


class SFTPDeleteFileOperator(BaseOperator):
    """
    This operator is used to delete a file from the SFTP server.

    :param sftp_conn_id: Connection Id in Airflow for the SFTP server.
    :param sftp_folder_path: Folder on SFTP server containing the file to be deleted.
    :param sftp_filename: Name of the file on SFTP server to delete, or unix wildcard characters to
                        specify specific or all files. If left empty all files in the folder will be
                        deleted.
    """

    template_fields = ["sftp_folder_path", "sftp_filename"]
    template_ext = []
    ui_color = "#f0eee4"

    @apply_defaults
    def __init__(
        self,
        sftp_conn_id: str,
        sftp_folder_path: str,
        sftp_filename: str = None,
        *args,
        **kwargs,
    ):
        super(SFTPDeleteFileOperator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_folder_path = sftp_folder_path
        self.sftp_filename = sftp_filename if sftp_filename else "*"

    def execute(self, context):
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        # Get list of files in sftp_path
        self.log.info(f"Getting list of files in sftp_path: `{self.sftp_folder_path}`")
        path_content = sftp_client.listdir(self.sftp_folder_path)
        files = [
            file for file in path_content if fnmatch.fnmatch(file, self.sftp_filename)
        ]
        sftp_object = None
        try:
            if not files:
                self.log.info(
                    f"No files found in folder that matches `{self.sftp_filename}` parameter."
                )
            for file in files:
                sftp_object = os.path.join(self.sftp_folder_path, file)
                sftp_client.remove(path=sftp_object)
                self.log.info(f"Deleted file `{sftp_object}`")
        except IOError as ex:
            # IOError raised by client does not consistently use the same
            # number of arguments when raised. When a file does not exist
            # the first argument is the error code `2`. If a folder is
            # passed then only a text error is used. If a permissions
            # error occurs then the first argument is error code 13.
            #
            # We only want to handle when a file does not exist, all other
            # exceptions should be reraised to fail the Airflow task.
            if ex.args[0] == 2:
                self.log.info(f"File does not exist `{sftp_object}`")
            else:
                raise
