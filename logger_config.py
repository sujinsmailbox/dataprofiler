import logging
from logging.handlers import TimedRotatingFileHandler


def get_logger(config):
    logger = logging.getLogger("root")

    if config.level == "debug":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    #ch.setLevel(config.level)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

    # # create file handler which logs even debug messages
    # if config.file_logging:
    #     fh = TimedRotatingFileHandler(
    #         "replicator_{}.log".format(config.replicator_instance_id), when="{}".format(config.file_logging_when),
    #         interval=config.file_logging_interval, backupCount=config.file_logging_backupcount
    #     )
    #     if config.debug:
    #         fh.setLevel(logging.DEBUG)
    #     else:
    #         fh.setLevel(logging.INFO)
    #     fh.setFormatter(formatter)
    #     logger.addHandler(fh)
    #
    # logger.info("Set console logging level to {}".format(config.console_logging_level))