from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
import csv
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
import http.client
import signal
from socketserver import ThreadingMixIn
import os, sys, argparse
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from common_functions import read_order_port, get_replica, get_frontend_host_port

# Class to hold shared attributes
class Memory_attributes:
    def __init__(self):
        # Initialize last order ID and a list to store new order logs
        self.last_order_id = 0
        self.order_logs = {}
        self.leader = 0
        self.replicas =[1,2,3]
        self.self_id = 0
        self.lock = threading.Lock()
        self.order_logs_file = None

# Instance to store shared attributes
params = Memory_attributes()

# Define the OrderService class to handle order-related operations
class OrderService(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Initialize a lock for thread safety
        self.lock = threading.Lock()
        super().__init__(*args, **kwargs)


    # Method to send a GET request to the catalog service for product quantity
    def get_catalog(self, product):
        # Establish connection with the catalog service
        catalog_ip = os.environ.get("CATALOG_IP") if os.environ.get("CATALOG_IP") else "localhost"
        catalog_conn = http.client.HTTPConnection(catalog_ip, 34567)
        catalog_conn.request("GET", f"/{product}")
        response = catalog_conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        return response
    
    # Method to send a POST request to the catalog service to update product quantity
    def post_catalog(self, json_data):
        # Establish connection with the catalog service
        catalog_ip = os.environ.get("CATALOG_IP") if os.environ.get("CATALOG_IP") else "localhost"
        catalog_conn = http.client.HTTPConnection(catalog_ip, 34567)
        json_data_str = json.dumps(json_data)
        product = json_data.get("name")
        catalog_conn.request("POST", f"/{product}", json_data_str, headers={"Content-type": "application/json"})
        response = catalog_conn.getresponse()
        response = response.read().decode()
        response = json.loads(response) 
        return response
    
    def post_order_sync(self,order):
        # Method to post latest order_logs to other replicas
        with self.lock:
            replicas = params.replicas
            if params.leader in replicas:
                replicas.remove(params.leader)
        print(f"Replicas to send order_logs - {replicas}")
        for id in replicas:
            
            print(f"Sending order to replica-{id}")
            order_ip, order_port = get_replica(str(id))
            order_conn = http.client.HTTPConnection(order_ip,order_port)
            json_data_str = json.dumps(order)
            order_conn.request("POST", f"/sync", json_data_str, headers={"Content-type": "application/json"})
            response = order_conn.getresponse()
            response = response.read().decode()
            response = json.loads(response) 
        return response

    def get_order_api(self,order_number):
        # API to fetch the order details 
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Acquire the lock and check if the order_number is present in order_logs
        with self.lock:
            print(params.order_logs)
            if order_number in params.order_logs:
                # If the order_number is found, construct the response data
                response_data = {
                    "message": "success", 
                    order_number: {
                        "name": params.order_logs[order_number]["name"],
                        "quantity": params.order_logs[order_number]["quantity"]
                    }
                }
            else:
                # If the product is not found, set an error message
                response_data = {"message": "error"}
            # Write the response data to the client
            print(response_data)
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
    
    def get_status_api(self):
         # API exposed to check if the service is up and running
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response_data = {"message": "service is up!"}
        self.wfile.write(json.dumps(response_data).encode('utf-8'))
    
    def get_last_order_id(self):
        # API exposed to get the last order ID
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response_data = {"last_order_id": params.last_order_id}
        self.wfile.write(json.dumps(response_data).encode('utf-8'))

    def sync_from_orderno_api(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data.decode('utf-8'))
        last_order_id = json_data.get("last_order_id")
        print(f"Syncing order_logs from order replica with last_order_id - {last_order_id}")
        response = {}
        #first sync from csv file for orders placed after last_order_id
        #then sync from order_logs for orders placed after last_order_id
        with open(params.order_logs_file, 'r') as file:
            print("Reading order_logs from disk")
            reader = csv.reader(file)
            for row in reader:
                print(row)
                if row is not None and int(row[0]) > last_order_id:
                    response[row[0]] = { "name": row[1], "quantity": row[2]}
        with self.lock:
            for key in params.order_logs.keys():
                if int(key) > int(last_order_id):
                    response[key] = params.order_logs[key]
        print(f"Sending order_logs to replica - {response}")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))



    def post_orders_api(self):
        # API to place an order
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data.decode('utf-8'))
        product = json_data.get("name")
        quantity = json_data.get("quantity")
    
        try: 
            # Get product information from the catalog service
            catalog_get_resp = self.get_catalog(product)
            catalog_quantity = catalog_get_resp[product]["quantity"]
            
            if catalog_get_resp["message"] == "success":
                print("Product present in catalog")
                # Check if the product is in stock
                if catalog_quantity < quantity:
                    print("Product not in stock in required quantity")
                    response = {"message": "fail"}
                else:
                    print(f"Product-{product} in stock with quantity - {catalog_quantity}")
                    # Post the order to the catalog service - to decrement the product quantity from catalog
                    catalog_post_resp = self.post_catalog(json_data)
                    if catalog_post_resp["message"] == "success":
                        with self.lock:
                            # Generate a new order ID and update the last_order_id
                            new_order_id = params.last_order_id + 1
                            response = {"message": "success", "order_id": new_order_id}
                            params.last_order_id = new_order_id
                            # Log the new order
                            params.order_logs[str(new_order_id)] = { "name": product, "quantity": quantity}
                            print(f"printing order_logs after insertion - {params.order_logs}")
                            send_response = { "order_id": str(new_order_id),
                                                "name": product, "quantity": quantity}
                        sync_response = self.post_order_sync(send_response)                                
                            
                        
                    else:
                        response = {"message": "error"}
            else:
                print("Product not present in catalog!")
                response = {"message": "error"}
        except Exception as e:
            print(f"Error encountered : {e}")   
                    
        # Send the HTTP response
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def post_sync_api(self):
        # API exposed to sync the order_logs after other order replicas placed orders
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data.decode('utf-8'))
        with self.lock:
            params.order_logs[json_data.get("order_id")] = { "name": json_data.get("name"), "quantity": json_data.get("quantity")}
            if params.last_order_id < int(json_data.get("order_id")):
                params.last_order_id = int(json_data.get("order_id"))
        print(f"Printing order_logs after sync - {params.order_logs}")
        response = {"message": "sync is successful!"}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def post_leader_api(self):
        # API exposed to get notified which order replica is the leader
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data.decode('utf-8'))
        leader = json_data.get("leader")
        with self.lock:
            params.leader = leader
        print(f"This is the order leader {leader}")
        response = {"message": "success"}
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps(response).encode('utf-8'))

    # Method to handle all GET APIs   
    def do_GET(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path.split('/')[1]=="status":
            self.get_status_api()
        
        elif parsed_path.path.split('/')[1]=="last":
            self.get_last_order_id()
           
        elif parsed_path.path.split('/')[1]=="syncfrom":
            #order_number = parsed_path.path.split('/')[-1]
            self.sync_from_orderno_api()
            
        else:
            order_number = parsed_path.path.split('/')[-1]
            print(f"information needed for order number = {order_number}")
            self.get_order_api(order_number)

    # POST request handler
    def do_POST(self):
        # Read the incoming POST data
        parsed_path = urlparse(self.path)
        if parsed_path.path.split('/')[1]=="orders":
            self.post_orders_api()

        elif parsed_path.path.split('/')[1]=="sync":
            self.post_sync_api()
            
        else:
            self.post_leader_api()
                


    # Finish method to clean up resources after request handling
    def finish(self):
        print(f"Finishing Current Task -{threading.current_thread().name}")
        super().finish()

# Define a subclass of HTTPServer with ThreadPoolExecutor support
class ThreadPoolHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, Loaded_Disk, *args, **kwargs):
        # Initialize ThreadPoolExecutor with max_workers
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.RequestHandlerClass = kwargs["RequestHandlerClass"]
        self.Loaded_Disk = Loaded_Disk
        print(Loaded_Disk) 
        super().__init__(*args, **kwargs)

    # Method to process incoming requests
    def process_request(self, request, client_address):
        print(f"process_request called from {client_address} ")
        client_address = client_address[0]  # Extract the client address from the tuple    
        # Submit each request to a new thread for concurrent handling
        self.executor.submit(self.__new_request_thread, request, client_address)
        
    # Method to create a new thread for each request
    def __new_request_thread(self, request, client_address):
        self.RequestHandlerClass(request, client_address, self)

    # Method to close the server and save changes to disk
    def server_close(self):
        self.Loaded_Disk._save_changes_to_disk()
        super().server_close()

# Class to load and save order logs to/from disk
class load_disk:
    def __init__(self, logs_file):
        self.logs_file = logs_file
        self._load_order_id_from_disk()

    # Method to load the last order ID from disk
    def _load_order_id_from_disk(self):
        try:
            with open(self.logs_file, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if row is not None:
                        params.last_order_id = row[0]
                # Convert last order ID to integer if it's not "Order_Id"
                params.last_order_id = int(params.last_order_id) if params.last_order_id!="Order_Id" else 0
                print(f"Last order ID loaded from disk: {params.last_order_id}")
        except FileNotFoundError as e:
            # Handle FileNotFoundError
            print(f"FileNotFound Exception : {e}")

    # Method to load order logs from disk
    def _load_order_logs_from_disk(self):
        try:
            with open(self.logs_file, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    data = {
                        "name": str(row[1]),
                        "quantity": int(row[2])
                    }
                    params.order_logs[row[0]] = data
            print(f"Order logs loaded from disk: {params.order_logs}")
        except FileNotFoundError:
            # If file not found, initialize empty catalog
            params.order_logs = {}

    # Method to save new order logs to disk
    def _save_changes_to_disk(self):
        print("Saving data to disk")
        try:
            with open(self.logs_file, 'a', newline='') as file:
                writer = csv.writer(file)
                for item, data in params.order_logs.items():
                    writer.writerow([item, data.get("name"), data.get("quantity")])
        except Exception as e:
            # Handle exceptions when saving to disk
            print(f"Error saving data to disk: {e}")

def check_and_sync():
    front_host, front_port = get_frontend_host_port()
    try:
    # Establish connection with the front end
        conn = http.client.HTTPConnection(front_host, front_port)
        conn.request("GET", "/leader")
        response = conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        leader = response.get("data")["leader"]
    except Exception as e:
        print(f"Frontend is not up yet - {e}")
        return
    
    if leader == 0:
        print("No leader elected yet, no sync required")
        return
    
    elif leader == params.self_id:
        #this will happen when this is the only replica alive, and it has crashed and restarted
        print("Leader is self, no sync required")
        return
    else:
        #leader is some other replica, sync may be required
        try:
            print(f"Checking if sync is required with leader - {leader}")
            # Establish connection with the leader
            leader_ip, leader_port = get_replica(str(leader))
            print(f"Leader is replica-{leader} with ip - {leader_ip} and port - {leader_port}")
            conn = http.client.HTTPConnection(leader_ip, leader_port)
            conn.request("GET", "/last", headers={"Content-type": "application/json"})
            response = conn.getresponse()
            response = response.read().decode()
            response = json.loads(response)
            leader_last_order_id = response.get("last_order_id")
            print(f"Leader's last order id - {leader_last_order_id}")
            if leader_last_order_id > params.last_order_id:
                #sync required
                print("Syncing order_logs from leader")
                conn.request("GET", "/syncfrom", headers={"Content-type": "application/json"}, body=json.dumps({"last_order_id":params.last_order_id}))
                response = conn.getresponse()
                response = response.read().decode()
                response = json.loads(response)
                print(f"Orders to be synced:{response}")
                with params.lock:
                    for key in response.keys():
                        params.order_logs[key] = response[key]
                    params.last_order_id = int(key)
                print(f"Order_logs after syncing:{params.order_logs}")
            else:
                print("No sync required")
        except Exception as e:
            print(f"Error while syncing with leader: {e}")
            return


# Method to run the server
def run_server(port, ORDER_LOGS):
    Loaded_Disk = load_disk(ORDER_LOGS)
    #check if sync is required
    check_and_sync()
    server_address = ('', port)
    httpd = ThreadPoolHTTPServer(Loaded_Disk, server_address, RequestHandlerClass=lambda *args, **kwargs: OrderService(*args, **kwargs))
    print('Order service running on port', port)
    #register signal handler for graceful shutdown when SIGTERM or SIGINT is received
    signal.signal(signal.SIGTERM, lambda x,_: shutdown(x, httpd))
    signal.signal(signal.SIGINT, lambda x,_: shutdown(x, httpd))
    #signal.signal(signal.SIGQUIT, lambda x,_: shutdown(x, httpd))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        #shutdown already handled by signal handler
        pass

def shutdown(sig, httpd):
    httpd.server_close()
    exit(0)


if __name__ == '__main__':
    # Define the port for the server to listen on
    parser = argparse.ArgumentParser(description="Order Service")

    # Add arguments with explicit names
    parser.add_argument('--port', type=int, help='Enter the port number for this order replica')
    parser.add_argument('--replica_id', type=int, help='Enter the id of this order replica')

    # Parse command-line arguments
    args = parser.parse_args()

    # Access parsed arguments
    print("PORT:", args.port)
    print("Replica_id:", args.replica_id)

    params.self_id = args.replica_id

    #PORT = 34568
    # Define the path to the order logs file
    #ORDER_LOGS_FILE = 'data/order_logs.csv'

    PORT = args.port
    if args.replica_id == 1:
        ORDER_LOGS_FILE = 'data/order_logs_1.csv'
    elif args.replica_id == 2:
        ORDER_LOGS_FILE = 'data/order_logs_2.csv'
    else:
        ORDER_LOGS_FILE = 'data/order_logs_3.csv'
    
    params.order_logs_file = ORDER_LOGS_FILE

    
    # Start the server
    run_server(PORT, ORDER_LOGS_FILE)
