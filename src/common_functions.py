import configparser, os

def read_order_port(order_id):
    config = configparser.RawConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'conf.properties')
    config.read(file_path)
    port = config["ORDER_REPLICA_PORTS"][str(order_id)]
    return port

def get_replica(id):
    parser = configparser.RawConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'conf.properties')
    parser.read(file_path)
    #return a tuple of host and port
    return (parser["ORDER_REPLICA_HOSTS"][str(id)], int(parser["ORDER_REPLICA_PORTS"][str(id)]))

def get_frontend_host_port():
    parser = configparser.RawConfigParser()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'conf.properties')
    parser.read(file_path)
    return (parser["FRONTEND"]["host"], int(parser["FRONTEND"]["port"]))