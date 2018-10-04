from copy import copy


class Model(object):
    def __repr__(self):
        return str({self.__class__.__name__: copy(self.__dict__)})

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__dict__)
