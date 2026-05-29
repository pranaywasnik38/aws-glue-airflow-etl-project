from datetime import datetime

def generate_timestamp():

    return datetime.now().strftime("%Y%m%d%H%M%S")