#!/usr/bin/env python

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable, Optional, Union

from pytools.common.logger import Logger


class ManagedTempFile:
    def __init__(self) -> None:
        self._logger = Logger(__name__)
        self.__temp_file_path: Optional[str] = None

    def get_temp_file(self, **kwargs: Any) -> str:
        """
        Returns path of a newly created temp file

        Arguments:
            kwargs - any optional arguments for NamedTemporaryFile

        Returns:
            Path of newly created temp file
        """
        with NamedTemporaryFile(delete=False, mode="w", encoding="UTF-8", **kwargs) as temp_file:
            self.__temp_file_path = temp_file.name
            self._logger.info(f"Temp file path: {self.__temp_file_path}")
            return self.__temp_file_path

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """
        Checks if the file exists

        Returns:
            True if object is a file and exists, else False
        """
        if os.path.isfile(file_path):
            return True
        Logger(__name__).info(f"{file_path} file not found")
        return False

    @staticmethod
    def delete_file(file_path: str) -> None:
        """
        Deletes file if it exists
        """
        if ManagedTempFile.file_exists(file_path):
            Logger(__name__).info(f"Deleting file: {file_path}")
            os.remove(file_path)

    @staticmethod
    def is_non_empty_file(file_path: str) -> bool:
        """
        Checks if the file is not empty

        Returns:
            True if file is not empty, else False
        """
        if ManagedTempFile.file_exists(file_path):
            if os.path.getsize(file_path) > 0:
                return True
            Logger(__name__).info(f"{file_path} is an empty file")
        return False


def relativize_path(path: Union[Path, str], base_paths: Iterable[Union[Path, str]]) -> Path:
    if isinstance(path, str):
        path = Path(path)
    for base_path in base_paths:
        try:
            return path.relative_to(base_path)
        except ValueError:
            # On Python 3.9 we can use `Path.is_relative_to` instead of catching `ValueError`.
            continue
    return path


logger = Logger(__name__)


def main() -> None:
    managed_temp_file = ManagedTempFile()
    temp_file = managed_temp_file.get_temp_file()
    # should return false because file is empty
    print(ManagedTempFile.is_non_empty_file(temp_file))
    ManagedTempFile.delete_file(temp_file)
    # should return false because file doesn't exists
    print(ManagedTempFile.is_non_empty_file(temp_file))


#######################################
if __name__ == "__main__":
    main()
