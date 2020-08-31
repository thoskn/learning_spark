import logging
import random
import sys
import os
from typing import Generator
from uuid import uuid4

import openfoodfacts
import requests

from data.formatter import (
    ItemCSVFormatter,
    PersonCSVFormatter,
    TransactionCSVFormatter,
)
from data.model import Item, Person, Thing, Transaction
from data.writer import CSVFileWriter, FileWriter


class EmptyCacheError(Exception):
    pass


class ExhaustedError(Exception):
    pass


class ThingGeneratingWritingRepository:
    def __init__(
        self,
        generator: Generator[Thing, None, None],
        writer: FileWriter,
        logger: logging.Logger,
    ):
        self._logger = logger
        self._generator = generator
        self._writer = writer
        self._thing_id_cache = []

    def get_random_new_thing_id(self) -> str:
        try:
            thing = next(self._generator)
        except StopIteration:
            raise ExhaustedError("No new things available")
        self._write(thing)
        self._thing_id_cache.append(thing.id)
        return thing.id

    def get_random_existing_thing_id(self) -> str:
        try:
            return random.choice(self._thing_id_cache)
        except IndexError:
            raise EmptyCacheError()

    def _write(self, thing: Thing):
        self._writer.write(thing)


def person_generator(
    logger: logging.Logger, batch_size: int
) -> Generator[Person, None, None]:
    while True:
        logger.info(f"Getting batch of {batch_size} from randomuser api")
        query_string = {"results": batch_size, "nat": "gb"}
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


def item_generator(logger: logging.Logger) -> Generator[Item, None, None]:
    for product in openfoodfacts.products.get_by_facets(
        {"country": "united kingdom", "category": "groceries"}
    ):
        # TODO check that this is iterating through all, not just single page
        # TODO more than just groceries category?? (would need to check not already returned)
        logger.debug(f"Item:\n{product}")
        yield Item(
            id=product["id"],
            # I don't want to deal with multiple categories
            category=product["categories"].split(",")[0],
            price=1.00,
        )


class BlockMessageFilterer:
    def __init__(self, msg_to_block: str):
        self._msg_to_block = msg_to_block

    def filter(self, record: logging.LogRecord) -> bool:
        return record.msg != self._msg_to_block


class DataSetBuilder:
    def __init__(
        self,
        logger: logging.Logger,
        write_batch_size: int,
        output_path: str,
        randomuser_api_batch_size: int,
    ):
        self._logger = logger
        self._write_batch_size = write_batch_size
        self._output_path = output_path
        self._randomuser_api_batch_size = randomuser_api_batch_size
        self._person_repository = self._get_person_repository()
        self._item_repository = self._get_item_repository()
        self._transaction_writer = self._get_transaction_writer()

    def build(self, size: int):
        for i in range(size):
            person_id = self._get_person_id()
            item_id = self._get_item_id()
            transaction = Transaction(
                id=str(uuid4()), person_id=person_id, item_id=item_id
            )
            self._transaction_writer.write(transaction)

    def _get_person_id(self) -> str:
        x = random.randint(0, 4)
        if x <= 3:
            try:
                return self._person_repository.get_random_existing_thing_id()
            except EmptyCacheError:
                pass
        return self._person_repository.get_random_new_thing_id()

    def _get_item_id(self) -> str:
        x = random.randint(0, 10)
        if x <= 8:
            try:
                return self._item_repository.get_random_existing_thing_id()
            except EmptyCacheError:
                pass
        try:
            return self._item_repository.get_random_new_thing_id()
        except ExhaustedError:
            msg = "No new items available, getting random existing item"
            self._logger.info(msg)
            self._logger.addFilter(BlockMessageFilterer(msg))
            return self._item_repository.get_random_existing_thing_id()

    def _get_person_repository(self):
        return ThingGeneratingWritingRepository(
            person_generator(self._logger, self._randomuser_api_batch_size),
            CSVFileWriter(
                self._logger,
                PersonCSVFormatter(),
                self._write_batch_size,
                os.path.join(self._output_path, "person"),
            ),
            self._logger,
        )

    def _get_item_repository(self):
        return ThingGeneratingWritingRepository(
            item_generator(self._logger),
            CSVFileWriter(
                self._logger,
                ItemCSVFormatter(),
                self._write_batch_size,
                os.path.join(self._output_path, "item"),
            ),
            self._logger,
        )

    def _get_transaction_writer(self):
        return CSVFileWriter(
            self._logger,
            TransactionCSVFormatter(),
            self._write_batch_size,
            os.path.join(self._output_path, "transaction"),
        )


if __name__ == "__main__":
    # API limit seems to be 20000 per minute
    RANDOMUSER_API_BATCH_SIZE = 100
    WRITE_BATCH_SIZE = 50000

    num_transactions = int(sys.argv[1])
    if len(sys.argv) > 2:
        log_level = sys.argv[2]
    else:
        log_level = "INFO"
    logging.basicConfig()
    logger_ = logging.getLogger(__file__)
    logger_.setLevel(log_level)

    DataSetBuilder(
        logger_, WRITE_BATCH_SIZE, "raw_data", RANDOMUSER_API_BATCH_SIZE
    ).build(num_transactions)
