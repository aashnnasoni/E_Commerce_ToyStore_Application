from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
import threading, json, time
import http.client
import os, sys
from socketserver import ThreadingMixIn
from LRUCache import LRUCache_Class
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
from common_functions import read_order_port, get_replica

LRUCache = LRUCache_Class(10)
global leader_id 
leader_lock = threading.Lock()


def notify_replicas(leader_id):
    """
    Method to notify all order replicas about the leader
    """
    replicas = [1,2,3]
    json_data = {
                    "leader": leader_id
                }
    json_data_str = json.dumps(json_data)
    print(f"This is replicas list:{replicas}")
    for id in replicas:
        try:
            print(f"Notifying id - {id}")
            replica_ip, replica_port = get_replica(str(id))
            replica_conn = http.client.HTTPConnection(replica_ip, replica_port)    
            replica_conn.request("POST", "/leader", json_data_str, headers={"Content-type": "application/json"})
            response = replica_conn.getresponse()
            response = response.read().decode()
            response = json.loads(response)
            print(f"Notification successful for {id}")
        except Exception as e:
            print(f"Notification failed for {id} with error: {e}")


def elect_leader():
    """
    Choose leader as the replica with the highest port number
    """
    id = 3
    while True:
        order_ip, order_port = get_replica(id)
        print(f"Trying replica {id} at order_port - {order_port}")
        try: 
            order_conn = http.client.HTTPConnection(order_ip, order_port)
            order_conn.request("GET", "/status")
            response = order_conn.getresponse().status
            print(response)
            if response == 200:
                print(f"Connection successful with order_replica id: {id}, hence leader elected!")
                with leader_lock:
                    global leader_id
                    leader_id = id
                # inform other order replicas about the leader
                notify_replicas(leader_id)
                break
        except Exception as e:
            print(f"Got an exception while connecting with order_replica id: {id} with error: {e}")
            id -= 1
            if id == 0:
                id = 3

# Function to make requests to backend services
def call_backend(request_type, product=None, order_number = None,request_body=None):
    # Establish connections to backend services
    catalog_ip = os.environ.get("CATALOG_IP") if os.environ.get("CATALOG_IP") else "localhost"
    #order_ip = os.environ.get("ORDER_IP") if os.environ.get("ORDER_IP") else "localhost"
    catalog_conn = http.client.HTTPConnection(catalog_ip, 34567)
    

    leader_ip, leader_port = get_replica(leader_id)
    print(f"Leader replica ip - {leader_ip} and port - {leader_port}, and leader_id - {leader_id}")
    try:
        order_conn = http.client.HTTPConnection(leader_ip, leader_port)
        order_conn.request("GET", "/status")
        response = order_conn.getresponse().status
    except Exception as e:
        print(f"Could not establish connection with leader_replica with error: {e}")
        print(f"Electing new leader")
        elect_leader()
        leader_ip, leader_port = get_replica(leader_id)
        order_conn = http.client.HTTPConnection(leader_ip, leader_port)
    

    print("Backend connection successfully established")
    if request_type == "GET" and product is not None:
        # Make a GET request to catalog service
        print("Make a GET request to catalog service")
        catalog_conn.request("GET", f"/{product}")
        response = catalog_conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        
    elif request_type == "GET" and order_number is not None:
        # Make a GET request to order service 
        print("Make a GET request to order service")
        order_conn.request("GET", f"/{order_number}")
        response = order_conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        
    else:
        # Make a POST request to order service
        print("Make a POST request to order service")
        json_data_str = json.dumps(request_body)
        order_conn.request("POST", f"/orders", json_data_str, headers={"Content-type": "application/json"})
        response = order_conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        
    return response



# Define a custom HTTP request handler class
class CustomHTTPRequestHandler(BaseHTTPRequestHandler):

    # GET request handler
    def do_GET(self):
        client_address = self.client_address
        print(f"This is the current thread- {threading.current_thread().name}")
        parsed_path = urlparse(self.path)
        if parsed_path.path.split('/')[1] == "products":
            product = parsed_path.path.split('/')[-1]
            self.get_products_api(product)
            
        elif parsed_path.path.split('/')[1] == "orders":
            order_number = parsed_path.path.split('/')[-1]
            self.get_orders_api(order_number)
        else:
            self.get_leader_api()

    # POST request handler
    def do_POST(self):
        client_address = self.client_address
        print(f"This is the current thread- {threading.current_thread().name}")
        parsed_path = urlparse(self.path)
        if parsed_path.path.split('/')[1] == "order":
            self.post_order_api()
        else:
            self.post_cache_api()

    def get_products_api(self, product):
        """
        This GET API fetches product details from backend services
        """
        cache_response = LRUCache.get(product)
        print(f"this is cache - {LRUCache.cache}")
        print(f"printing cache response - {cache_response}")
        if cache_response!= -1:
            print(f"product present in cache")
            response = {
                "data": {product: cache_response}
            }
        else:    
            # Call backend service to get product information
            response = call_backend(request_type="GET", product=product)
            # Prepare response based on backend response
            if response.get("message") == "error":
                response = {
                    "error": {
                        "code": 404,
                        "message": "product not found"
                    }
                }
            else:
                LRUCache.put(product,response[product])
                print(f"this is cache after insertion - {LRUCache.cache}")
                response = {
                    "data": {product: response[product]}
                }
                
            # Setting response headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Send JSON response
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def get_orders_api(self, order_number):
        """
        This GET API fetches order details from backend services
        """
        # Call backend service to get order information
        response = call_backend(request_type="GET", order_number=order_number)
        # Prepare response based on backend response
        if response.get("message") == "error":
            response = {
                "error": {
                    "code": 404,
                    "message": "order_number not found"
                }
            }
        else:
            response = {
                "data": {"number" : order_number,
                            "name": response[order_number]["name"],
                            "quantity":response[order_number]["quantity"] }
            }
        # Setting response headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Send JSON response
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def get_leader_api(self):
        """
        This GET API fetches leader details
        """
        with leader_lock:
            response = {
                "data": {
                    "leader": leader_id
                }
            }
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Send JSON response
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def post_order_api(self):
        """
        This POST API posts order requests to backend services
        """
        content_length = int(self.headers['Content-Length'])
        # Get the data
        post_data = self.rfile.read(content_length)
        # Parse the data to JSON
        json_data = json.loads(post_data.decode('utf-8'))
        print(json_data)
        # Call backend service to place order
        response = call_backend(request_type="POST", request_body=json_data)
        # Prepare response based on backend response
        if response.get("message") == "success":
            response = {
                "message": "Order Placed!",
                "data": {
                    "order_number": response.get("order_id")
                }
            }
        elif response.get("message") == "fail":
            response = {
                "error": {
                    "code": 417,
                    "message": "product not in stock in desired quantity"
                }
            }
        else:
            response = {
                "error": {
                    "code": 404,
                    "message": "product not found"
                }
            }
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Send JSON response
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def post_cache_api(self):
        """
        This POST API is used by catalog backend service to clear the cache for a given product
        """
        content_length = int(self.headers['Content-Length'])
        #Get the data
        post_data = self.rfile.read(content_length)
        # Parse the data to JSON
        json_data = json.loads(post_data.decode('utf-8'))
        print(json_data)
        product = json_data["name"]
        # Remove the given product from cache
        LRUCache.remove(product)
        response = { "message" : "successly removed product from cache"}
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Send JSON response
        self.wfile.write(json.dumps(response).encode('utf-8'))
    


    # Overriding finish method to release thread on connection close
    def finish(self):
        print(f"Finishing Current Task -{threading.current_thread().name}")
        super().finish()

# Define a subclass of HTTPServer with ThreadPoolExecutor support
class ThreadPoolHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, *args, **kwargs):
        self.executor = ThreadPoolExecutor(max_workers=5)  # Adjust max_workers as needed
        super().__init__(*args, **kwargs)

    # Method to process incoming requests
    def process_request(self, request, client_address):
        print(f"process_request called from {client_address} ")
        client_address = client_address[0]  # Extract the client address from the tuple
        self.executor.submit(self.__new_request_thread, request, client_address)

    # Method to create a new thread for each request
    def __new_request_thread(self, request, client_address):
        self.RequestHandlerClass(request, client_address, self)

# Run the server
def run(server_class=ThreadPoolHTTPServer, RequestHandlerClass=CustomHTTPRequestHandler, port=34569):
    server_address = ('', port)
    httpd = server_class(server_address, RequestHandlerClass=RequestHandlerClass)
    print(f'Starting server on port {port}...')

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    leader_id = 3 # Initially set leader as replica 3 (highest port number)
    elect_leader()
    run()