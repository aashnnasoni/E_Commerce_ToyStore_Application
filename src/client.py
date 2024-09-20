import http.client
import random, time, json

# Parameters for order probability and number of queries
order_probability = 0.5
num_queries = 3
all_orders = {}

def compare_order_data(conn):
    
    # Compare local order data with data from front-end
    for order_number, order_data in all_orders.items():
        conn.request("GET", f"/orders/{order_number}")
        response = conn.getresponse()
        response = response.read().decode()
        response = json.loads(response)
        print(response)
        response_data = response.get("data")
        if response_data is not None:
            if response_data.get("name") == order_data.get("name") and response_data.get("quantity") == order_data.get("quantity"):
                print ( f" Order_Number - {order_number} Information matched.")
            else:
                print ( f" Order_Number - {order_number} Information not matched!")
        else : 
            print (f" Order_Number - {order_number} not found!")

# Function to make requests to the server
def make_request(conn):
    # List of products
    products = ["Whale", "Tux", "Santa", "Python", "Fox", "Kite", 
                "Lion", "Slinky", "DollHouse", "Dominoes", "Bubbles", 
                "Twisters", "HotWheels","PinBall", "Jenga", "Scrabble","Cobra"]

    # Loop to make multiple requests
    for _ in range(num_queries):
        # Randomly choosing a product from the list
        index_var = random.randint(0, len(products) - 1)
        product_string = products[index_var]
        #product_string = "Whale"
        response = None
        try:
            # Making a GET request to retrieve product information
            conn.request("GET", f"/products/{product_string}")
            #conn.request("GET", f"/products/Whale")
            response = conn.getresponse()
            response = response.read().decode()
            response = json.loads(response)
        except Exception as e:
            print(f"Error while making GET Request: {e} - Retrying to estabilish connection ")
            conn.request("GET", f"/products/{product_string}")
        
            response = conn.getresponse()
            response = response.read().decode()
            response = json.loads(response)
        print()
        print(response)

        # If product data is available
        if response.get("data") is not None:
            available_quantity = response["data"][product_string]["quantity"]
            quantity_required = int(random.random()*20)
            # If product is available and order probability is met, place an order
            if available_quantity > quantity_required and random.random() >= order_probability:
                json_data = {
                    "name": product_string,
                    "quantity": quantity_required
                }
                json_data_str = json.dumps(json_data)
                # Making a POST request to place an order
                try: 
                    conn.request("POST", "/order", json_data_str, headers={"Content-type": "application/json"})
                    response = conn.getresponse()
                    response = response.read().decode()
                    response = json.loads(response)
                    print(response)
                    if response.get("data") is not None:
                        all_orders[response.get("data").get("order_number")] = {"name" : product_string,
                                      "quantity": quantity_required }


                except Exception as e:
                    print(f"Error while making POST Request: {e} - Retrying to estabilish connection")
                    conn.request("POST", "/order", json_data_str, headers={"Content-type": "application/json"})
                    response = conn.getresponse()
                    print(response.read().decode())

    compare_order_data(conn)
    


if __name__ == "__main__":
    try:
        # Establishing connection with the server
        conn = http.client.HTTPConnection('localhost', 34569)
    except Exception as e:
        print(f"Could not establish connection with front_end with error: {e}") 

    # Making requests
    make_request(conn)

    # Closing the connection
    conn.close()
