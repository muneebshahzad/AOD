import concurrent,os
import shopify,time
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from concurrent.futures import ThreadPoolExecutor
import pymssql
app = Flask(__name__)
app.secret_key = 'your_secret_key'
# Set up the Shopify session
shop_url = "https://08dc7a.myshopify.com/admin"
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.set_user(os.environ.get('SHOPIFY_USER'))
shopify.ShopifyResource.set_password(os.environ.get('SHOPIFY_PASSWORD'))

# Function to fetch order statuses in parallel
def get_order_statuses(tracking_numbers):
    try:
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit each API call to the executor
            futures = {executor.submit(fetch_status, tracking_number): tracking_number for tracking_number in tracking_numbers}
            for future in concurrent.futures.as_completed(futures):
                tracking_number = futures[future]
                try:
                    data = future.result()
                    results.append((tracking_number, data))
                except Exception as e:
                    print(f"Error fetching order status for tracking number {tracking_number}: {e}")
                    results.append((tracking_number, "Error"))
        return dict(results)
    except Exception as e:
        print(f"Error fetching order statuses: {e}")
        return {}

# Fetch status for a single tracking number
def fetch_status(tracking_number):
    try:
        url = f"https://cod.callcourier.com.pk/api/CallCourier/GetTackingHistory?cn={tracking_number}"
        response = requests.get(url)
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            last_status = data[-1]["ProcessDescForPortal"]
            return last_status
        else:
            return "Unknown"
    except Exception as e:
        raise Exception(f"Error fetching order status for tracking number {tracking_number}: {e}")

# Modify the get_orders function to include logging
@app.route('/')
def get_orders():
    start_time = time.time()  # Record the start time

    pending_orders_count = 0
    delivered_orders_count = 0
    undelivered_orders_count = 0
    returned_orders_count = 0
    oldest_pending_days = 0
    created_at_min = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S%z')

    # Set created_at_max to the current datetime
    created_at_max = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')

    # Retrieve orders within the date range
    orders_start_time = time.time()  # Record the start time for order retrieval
    orders = shopify.Order.find(order='created_at DESC', created_at_min=created_at_min, created_at_max=created_at_max)
    orders_end_time = time.time()  # Record the end time for order retrieval
    print(f"Time taken for order retrieval: {orders_end_time - orders_start_time} seconds")  # Log time taken

    # Extract tracking numbers from orders
    tracking_numbers = [fulfillment.tracking_number for order in orders for fulfillment in order.fulfillments]

    # Fetch order statuses in parallel
    order_statuses = get_order_statuses(tracking_numbers)

    # Process orders
    formatted_orders = []
    for order in orders:
        items = []

        trackingnum = 'N/A'
        trackingurl = None
        status = "Pending"
        courier = "N/A"

        for fulfillment in order.fulfillments:
            trackingurl = fulfillment.tracking_url
            trackingnum = fulfillment.tracking_number

        for line_item in order.line_items:
            try:
                title = line_item.title + " - " + line_item.variant_title
            except:
                title = line_item.title

            quantity = line_item.quantity
            product = shopify.Product.find(line_item.product_id)

            try:
                for variant in product.variants:
                    if variant.id == line_item.variant_id:
                        images = shopify.Image.find(product_id=line_item.product_id)
                        for image in images:
                            if line_item.variant_title is not None:
                                if image.id == variant.image_id:
                                    img_url = image.src
                                    break
                                else:
                                    img_url = None
                            else:
                                img_url = image.src
            except:
                img_url = image.src

            items.append({
                "name": title,
                "quantity": quantity,
                "img_url": img_url
            })

        if trackingnum == 'N/A':
            pending_orders_count += 1
        else:
            status = order_statuses.get(trackingnum, "Unknown")
            if status == "Booked & Pending" or status == "Pending":
                pending_orders_count += 1
            elif status == "DELIVERED":
                delivered_orders_count += 1
            elif status == "RETURN SUBMITTED":
                returned_orders_count += 1
            else:
                undelivered_orders_count += 1
            courier = "CCS"

        pending_since = datetime.now(timezone.utc) - datetime.strptime(order.created_at, '%Y-%m-%dT%H:%M:%S%z')
        pending_since = pending_since.days
        if pending_since > oldest_pending_days:
            oldest_pending_days = pending_since

        formatted_order = {
            'order_no': order.order_number,
            'tracking_no': trackingnum,
            'tracking_url': trackingurl,
            'order_date': order.created_at,  # Corrected line
            'price': order.total_price,
            'items': items,
            'order_status': status,
            'pending_since': pending_since,
            'order_via': 'Shopify',
            'shipped_via': courier,
            'tracking_link': trackingurl
        }

        formatted_orders.append(formatted_order)

    # Calculate and log the total time taken for order processing
    end_time = time.time()
    print(f"Total time taken for order processing: {end_time - start_time} seconds")

    total_orders_count = pending_orders_count + delivered_orders_count + undelivered_orders_count
    delivery_ratio = 0
    if total_orders_count != 0:
        delivery_ratio = int((delivered_orders_count / total_orders_count) * 100)

    return render_template('orders_home.html', orders=formatted_orders, pending_orders_count=pending_orders_count,
                           delivered_orders_count=delivered_orders_count, undelivered_orders_count=undelivered_orders_count,
                           oldest_pending_days=oldest_pending_days, returned_orders_count=returned_orders_count,
                           delivery_ratio=delivery_ratio)


def check_database_connection():
    server = os.environ.get('SERVER')
    database = os.environ.get('DATABASE_NAME')
    username = os.environ.get('DATABASE_USERNAME')
    password = os.environ.get('DATABASE_PASSWORD')

    try:
        print('Connecting to the database...')
        connection = pymssql.connect(server=server, user=username, password=password, database=database)
        print('Connected to the database')
        return connection
    except pymssql.Error as e:
        print(f"Error connecting to the database: {str(e)}")
        return None

@app.before_request
def require_login():
    print('Endpoint:', request.endpoint)
    allowed_routes = ['login', 'static']
    if request.endpoint not in allowed_routes and 'user_id' not in session:
        print('Redirecting to login')
        return redirect(url_for('login'))
##LOGIN

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        print('Login POST request received')
        username = request.form.get('username')
        password = request.form.get('password')

        # Assuming you have a SQL Server database connection
        connection = check_database_connection()

        try:
            if connection:
                cursor = connection.cursor()
                query = "SELECT * FROM users WHERE username = %s AND password = %s"
                cursor.execute(query, (username, password))
                user = cursor.fetchone()

                if user:
                    # If the user exists, store their information in the session
                    session['user_id'] = user[0]  # Assuming user_id is the first column
                    session['username'] = user[1]  # Assuming username is the second column
                    # You can store more user-related information in the session if needed

                    return redirect(url_for('get_orders'))  # Redirect to the home page or another authenticated route
                else:
                    # If the login fails, you can show an error message
                    error_message = "Invalid username or password"
                    return render_template('Login1.html', error_message=error_message)
            else:
                return "Error: No database connection"

        except Exception as e:
            print(f"Error during login: {str(e)}")
            return "Error during login"

        finally:
            if connection:
                connection.close()

    return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True)
