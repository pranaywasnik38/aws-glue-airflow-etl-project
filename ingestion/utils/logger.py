import logging

def get_logger(module_name):

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s : %(message)s"
    )

    return logging.getLogger(module_name)