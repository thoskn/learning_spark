import abc
import logging
import os
from pathlib import Path
from uuid import uuid4

from data.formatter import CSVFormatter, Formatter
from data.model import Thing


class FileWriter(abc.ABC):
    def __init__(
        self,
        logger: logging.Logger,
        formatter: Formatter,
        max_records_per_file: int,
        output_path: str,
    ):
        self._logger = logger
        self._formatter = formatter
        self._max_records_per_file = max_records_per_file
        self._output_path = output_path
        # counter used for knowing when to write out to file
        self._records_in_file = 0

    @abc.abstractmethod
    def write(self, thing: Thing):
        pass


class CSVFileWriter(FileWriter):
    def __init__(
        self,
        logger: logging.Logger,
        formatter: CSVFormatter,
        records_per_file: int,
        output_path: str,
    ):
        super().__init__(logger, formatter, records_per_file, output_path)
        #  No functional benefit to this, just adds the CSVFormatter type hint
        self._formatter = formatter
        self._csv = ""

    def write(self, thing: Thing):
        if self._records_in_file == 0:
            self._csv = self._formatter.get_header()
        self._csv += self._formatter.format(thing)
        self._records_in_file += 1
        if self._records_in_file == self._max_records_per_file:
            self._write_file()
            self._records_in_file = 0

    def _write_file(self):
        full_file_path = os.path.join(self._output_path, f"{uuid4()}.csv")
        self._logger.info(f"Saving results to file: {full_file_path}")
        Path(self._output_path).mkdir(parents=True, exist_ok=True)
        with open(full_file_path, "w+") as f:
            f.write(self._csv)
            self._csv = ""

    def __del__(self):
        if self._csv:
            self._write_file()
