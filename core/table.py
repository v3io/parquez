import abc


class Table(abc.ABC):
    @abc.abstractmethod
    def create(self):
        pass

    @abc.abstractmethod
    def drop(self):
        pass
