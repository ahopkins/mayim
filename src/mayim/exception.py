class MayimError(Exception):
    pass


class RecordNotFound(MayimError):
    pass


class MissingSQL(MayimError):
    pass
