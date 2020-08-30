import abc
import logging
import sys
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Generator
from uuid import uuid4

import requests


@dataclass
class Person:
    id: str
    title: str
    first_name: str
    last_name: str
    age: int
    gender: str


@dataclass
class Transaction:
    id: str
    person_id: str
    item_id: str


@dataclass
class Item:
    id: str
    price: str
    category: str


class Formatter(abc.ABC):
    @abc.abstractmethod
    def format(self, thing: object):
        pass


class CSVFormatter(Formatter):
    @abc.abstractmethod
    def get_header(self) -> str:
        pass


class PersonCSVFormatter(CSVFormatter):
    def format(self, person: Person) -> str:
        return f"{person.id},{person.title},{person.first_name},{person.last_name},{person.age},{person.gender}\n"

    def get_header(self) -> str:
        return "ID,TITLE,FIRST_NAME,LAST_NAME,AGE,GENDER\n"


class Writer(abc.ABC):
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @abc.abstractmethod
    def write(
        self,
        generator: Generator[object, None, None],
        formatter: Formatter,
        records_per_file: int,
        output_path: str,
    ):
        pass


class CSVWriter(Writer):
    def write(
        self,
        generator: Generator[object, None, None],
        formatter: CSVFormatter,
        records_per_file: int,
        output_path: str,
    ):
        csv = formatter.get_header()
        record_counter = 0
        for item in generator:
            csv += formatter.format(item)
            record_counter += 1
            if record_counter == records_per_file:
                self._write_file(csv, output_path)
        self._write_file(csv, output_path)

    def _write_file(self, csv: str, output_path: str):
        full_file_path = os.path.join(output_path, f"{uuid4()}.csv")
        self._logger.info(f"Saving results to file: {full_file_path}")
        Path(output_path).mkdir(parents=True, exist_ok=True)
        with open(full_file_path, "w+") as f:
            f.write(csv)


def person_generator(
    size: int, logger: logging.Logger, batch_size: int
) -> Generator[Person, None, None]:
    yielded = 0
    while yielded < size:
        current_batch_size = (
            batch_size if yielded + batch_size <= size else size - yielded
        )
        logger.info(f"Getting batch of {current_batch_size} from randomuser api")
        query_string = {"results": current_batch_size, "nat": "gb"}
        response = requests.get("https://randomuser.me/api/", params=query_string)
        # I have open a PR (https://github.com/RandomAPI/Randomuser.me-Node/pull/177)
        # for having the correct response code for rate limit exceeded
        # so that a pause and retry can be implemented here
        try:
            response.raise_for_status()
        except:
            # This is a bit of a strange way of doing this
            # but _sometimes_ the response fails to be deserialised to json on error codes
            try:
                response = response.json()
                logger.error(
                    f"Non-success code returned from randomuser API.\n"
                    f"Response: {response}"
                )
            except:
                pass
            raise
        response = response.json()
        logger.debug(f"randomuser response:\n{response}")
        for r in response["results"]:
            name = r["name"]
            yield Person(
                id=str(uuid4()),
                title=name["title"],
                first_name=name["first"],
                last_name=name["last"],
                age=r["dob"]["age"],
                gender=r["gender"],
            )
            yielded += 1


def create_data_set(
    size: int,
    logger: logging.Logger,
    writer: Writer,
    output_path: str,
    api_batch_size: int,
    write_batch_size: int,
):
    writer.write(
        person_generator(size, logger, api_batch_size),
        PersonCSVFormatter(),
        write_batch_size,
        str(os.path.join(output_path, "person")),
    )


if __name__ == "__main__":
    # API limit seems to be 20000 per minute
    RANDOMUSER_API_BATCH_SIZE = 100
    WRITE_BATCH_SIZE = 50000

    num_people = int(sys.argv[1])
    if len(sys.argv) > 2:
        log_level = sys.argv[2]
    else:
        log_level = "INFO"
    logging.basicConfig()
    logger = logging.getLogger(__file__)
    logger.setLevel(log_level)

    create_data_set(
        num_people,
        logger,
        CSVWriter(logger),
        "raw_data",
        RANDOMUSER_API_BATCH_SIZE,
        WRITE_BATCH_SIZE,
    )
