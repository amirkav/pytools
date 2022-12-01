#!/usr/bin/env python
import uuid

from pytools.common.logger import Logger


#######################################
class UuidGenerator:
    ###################

    @staticmethod
    def generate_uuid() -> uuid.UUID:
        """
        https://docs.python.org/3/library/uuid.html
        """
        return uuid.uuid4()

    @classmethod
    def generate_uuid_str(cls) -> str:
        id_str = str(cls.generate_uuid()).replace("-", "")
        return id_str


#######################################
def main() -> None:
    logger = Logger(__name__, level=Logger.DEBUG)
    ugen = UuidGenerator()
    my_id = ugen.generate_uuid()
    logger.debug(str(type(my_id)))
    logger.debug(str(my_id))
    # logger.debug("uuid.uuid4(): {}".format(id))
    # logger.debug("id.bytes: {}".format(id.bytes))
    # logger.debug("id.int: {}".format(id.int))
    # logger.debug("id.hex: {}".format(id.hex))
    # logger.debug("id.version: {}".format(id.version))
    # logger.debug("id.variant:{}".format(id.variant))
    # logger.debug("id.fields:{}".format(id.fields))
    # logger.debug("id.node: {}".format(id.node))

    my_uuid = ugen.generate_uuid_str()
    logger.debug("uuid: {}".format(my_uuid))


if __name__ == "__main__":
    main()
