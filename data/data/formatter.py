import abc

from data.model import Item, Person, Transaction


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


class ItemCSVFormatter(CSVFormatter):
    def format(self, item: Item) -> str:
        return f"{item.id},{item.price},{item.category}\n"

    def get_header(self) -> str:
        return "ID,PRICE,CATEGORY\n"


class TransactionCSVFormatter(CSVFormatter):
    def format(self, transaction: Transaction) -> str:
        return f"{transaction.id},{transaction.person_id},{transaction.item_id}\n"

    def get_header(self) -> str:
        return "ID,PERSON_ID,ITEM_ID\n"
