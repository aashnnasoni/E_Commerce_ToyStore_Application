from http.server import BaseHTTPRequestHandler, HTTPServer
import http.client
import json
import threading
import csv
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
from socketserver import ThreadingMixIn
import signal
import os
import time

# Class to hold shared attributes
class Memory_attributes:
    def __init__(self):
        # Initialize an empty dictionary to hold catalog data
        self.catalog_data = {}
        #self.lock = threading.Lock()

# Instance to store shared attributes
params = Memory_attributes()

def clear_cache(product):
    front_end_conn = http.client.HTTPConnection('localhost', 34569)
    json_data = {
                    "name": product,
                }
    json_data_str = json.dumps(json_data)
    front_end_conn.request("POST", "/cache", json_data_str, headers={"Content-type": "application/json"})
    response = front_end_conn.getresponse()
    response = response.read().decode()
    response = json.loads(response)
    print(response)

# Define the CatalogService class to handle catalog-related operations
class CatalogService(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Initialize a lock for thread safety
        self.lock = threading.Lock()
        # Print the current catalog data
        print(f"Printing catalog_data:{params.catalog_data}")
        super().__init__(*args, **kwargs)

    # Method to handle GET requests
    def do_GET(self):
        # Print the current thread handling the request
        print(f"This is the current thread- {threading.current_thread().name}")
        # Parse the request path to extract the product
        parsed_path = urlparse(self.path)
        product = parsed_path.path.split('/')[-1]
        # Send the HTTP response headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        # Acquire the lock and check if the product is in the catalog data
        with self.lock:
            if product in params.catalog_data:
                # If the product is found, construct the response data
                response_data = {
                    "message": "success", 
                    product: {
                        "quantity": params.catalog_data[product]["quantity"],
                        "price": params.catalog_data[product]["price"]
                    }
                }
            else:
                # If the product is not found, set an error message
                response_data = {"message": "error"}
            # Write the response data to the client
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
    
    # Method to handle POST requests
    def do_POST(self):
        # Read the content length of the incoming POST data
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        # Parse the JSON data
        json_data = json.loads(post_data.decode('utf-8'))
        message = None
        # Acquire the lock and process the JSON data
        with self.lock:
            product = json_data.get("name")
            quantity = json_data.get("quantity")
            if product in params.catalog_data and params.catalog_data[product]["quantity"] >= quantity:
                # If the product is found, update the quantity
                params.catalog_data[product]["quantity"] -= quantity
                message = "success"
                clear_cache(product)
            else:
                # If the product is not found, set an error message
                message = "error"
        # Send the HTTP response
        self.send_response(200)
        self.end_headers()
        response = {"message": message}
        # Write the response data to the client
        self.wfile.write(json.dumps(response).encode('utf-8'))

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

# Class to load and save catalog data to/from disk
class load_disk:
    def __init__(self, catalog_file):
        self.catalog_file = catalog_file
        self._load_catalog_from_disk()

    # Method to load catalog data from disk
    def _load_catalog_from_disk(self):
        try:
            with open(self.catalog_file, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    data = {
                        "quantity": int(row[1]),
                        "price": float(row[2])
                    }
                    params.catalog_data[row[0]] = data
        except FileNotFoundError:
            # If file not found, initialize empty catalog
            params.catalog_data = {}

    # Method to save changes to catalog data to disk
    def _save_changes_to_disk(self):
        print("Saving data to disk")
        try:
            with open(self.catalog_file, 'w', newline='') as file:
                writer = csv.writer(file)
                for item, data in params.catalog_data.items():
                    writer.writerow([item, data.get("quantity"), data.get("price")])
        except Exception as e:
            # Handle exceptions when saving to disk
            print(f"Error saving data to disk: {e}")

def restock_catalog():
    t = threading.current_thread()
    while getattr(t, "run", True):
        #run every 10 seconds
        time.sleep(10)
        for product in params.catalog_data:
            if params.catalog_data[product]["quantity"] == 0:
                params.catalog_data[product]["quantity"] = 100
                print(f"Restocked {product} to 100")
                #ask front end to clear cache
                clear_cache(product)

# Method to run the server
def run_server(port, CATALOG_FILE):
    # Load catalog data from disk
    Loaded_Disk = load_disk(CATALOG_FILE)
    server_address = ('', port)
    # Initialize and start the HTTP server
    httpd = ThreadPoolHTTPServer(Loaded_Disk, server_address, RequestHandlerClass=lambda *args, **kwargs: CatalogService(*args, **kwargs))
    print('Catalog service running on port', port)

    #start restocking thread
    restock_thread = threading.Thread(target=restock_catalog)
    restock_thread.start()

    #register signal handlers
    signal.signal(signal.SIGTERM, lambda x,_: shutdown(x, httpd, restock_thread))
    signal.signal(signal.SIGINT, lambda x,_: shutdown(x, httpd, restock_thread))
    #signal.signal(signal.SIGQUIT, lambda x,_: shutdown(x, httpd))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # exits already handled by signal handlers
        pass

def shutdown(sig, httpd, restock_thread):
    '''
    clean up and shutdown server
    '''
    print(f"Received signal {sig}, shutting down server")
    # wait for restock thread to finish
    restock_thread.run = False
    httpd.server_close()
    exit(0)


if __name__ == '__main__':
    # Define the port for the server to listen on
    PORT = 34567
    # Define the path to the catalog file
    CATALOG_FILE = 'data/catalog.csv'
    # Start the server
    run_server(PORT, CATALOG_FILE)
