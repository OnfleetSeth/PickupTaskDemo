import json
import requests
import utilities
import queryDB as q
from ratelimit import limits

api_key = "b4523670d4ba81be1c6a2084776093eb"

# Assumption is that pickup tasks are at a hub (hence, multiple pickups)
# "Destination" on pickup task should be the address of the Hub
def get_hub_address():
    url = "https://onfleet.com/api/v2/hubs"

    payload = {}
    headers = {
        'Authorization': 'Basic ' + utilities.encode_b64(api_key)
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)
    addy = (json.loads(response.text))[0]['address']
    # print(addy)
    return addy

# Create tasks for new delivery orders in the orders DB
def create_new_tasks():
    url = "https://onfleet.com/api/v2/tasks"

    orders = q.new_orders()
    task_list = []

    for t in orders:
        barcode = utilities.encode_b64(t['barcode_data'])

        if t['apt'] is None:
            payload = json.dumps({"destination": {"address": {"number": t['number'], "street": t['street_name'],
                                "apartment": "", "city": t['city'], "state": t['state'], "country": t['country']}},
                                "recipients": [{"name": t['rec_name'], "phone": t['rec_phone']}],"completeAfter":
                                t['complete_after'], "completeBefore": t['complete_before'], "notes": t['notes'],
                                "metadata": [{"name": "Order Number","type": "string", "value": t['order_number'],
                                "visibility": ["api", "dashboard"]}], "barcodes": [{"data": barcode,
                                                                                    "blockCompletion":"true"}]})
            headers = {
                'Authorization': 'Basic ' + utilities.encode_b64(api_key),
                'Content-Type': 'application/json'
            }
            response = requests.request("POST", url, headers=headers, data=payload)

            text = dict(json.loads(response.text))

            id = {"taskId": text['id'], "order_number": text['metadata'][0]['value']}
            task_list.append(id)

        else:
            payload = json.dumps({"destination": {"address": {"number": t['number'], "street": t['street_name'],
                                "apartment": t['apt'], "city": t['city'], "state": t['state'],"country": t['country']}},
                                "recipients": [{"name": t['rec_name'], "phone": t['rec_phone']}], "completeAfter":
                                t['complete_after'], "completeBefore": t['complete_before'], "notes": t['notes'],
                                "metadata": [{"name": "Order Number", "type": "string", "value": t['order_number'],
                                "visibility": ["api", "dashboard"]}], "barcodes": [{"data": barcode,
                                "blockCompletion": "true"}]})
            headers = {
                'Authorization': 'Basic ' + utilities.encode_b64(api_key),
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)

            text = dict(json.loads(response.text))
            id = text['id']
            task_list.append(id)

    q.update_order_with_task_id(task_list)

    return task_list

# Check to see if a task is a pickup task.
# - We don't want to generate pickups for pickups
# - Quick evaluation to check whether a pickup task already exists and just needs updated

@limits(calls=20, period=1)
def pickup_check(taskid):

    taskId = taskid
    url = "https://onfleet.com/api/v2/tasks/" + taskId

    payload = {}
    headers = {
        'Authorization': 'Basic ' + utilities.encode_b64(api_key)
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    dict_response = dict(json.loads(response.text))

    pickup_boolean = dict_response['pickupTask']

    # print(pickup_boolean)
    return pickup_boolean

# We know a worker has been assigned 1+ tasks, so let's get those tasks
# We go to the worker and not the database to preserve task order
def get_worker_tasks(workerid, taskindex=None):
    workerid = workerid
    task_index = taskindex

    url1 = "https://onfleet.com/api/v2/workers/"
    analytics = "?analytics=false" # No need for analytics
    url2 = url1 + workerid + analytics

    payload = ""
    headers = {
        'Authorization': 'Basic ' + utilities.encode_b64(api_key)
    }

    response = requests.request("GET", url2, headers=headers, data=payload)

    dict_response = dict(json.loads(response.text))

    if task_index is None:
        delivery_tasks = []
        all_tasks = dict_response['tasks']

        for task in all_tasks:

            is_pickup = pickup_check(task)

            if is_pickup is False:
                delivery_tasks.append(task)


        print("Driver's delivery tasks: " + str(delivery_tasks))
        return delivery_tasks

    else:
        task = dict_response['tasks'][0]
        # print("First task is " + task)
        return task

# Once we know what tasks the driver has been assigned, we need the details from those tasks for the new pickup task
def get_delivery_deets(worker_tasks):

    tasks = worker_tasks

    url = "https://onfleet.com/api/v2/tasks/"
    delivery_deets = []

    for task_id in tasks:
        url2 = url + task_id

        payload={}
        headers = {
          'Authorization': 'Basic ' + utilities.encode_b64(api_key)
        }

        response = requests.request("GET", url2, headers=headers, data=payload)


        dict_response = dict(json.loads(response.text))

        pickup_dict = {"order_number": dict_response['metadata'][0]['value'],"barcode": dict_response['barcodes']
                      ['required'][0]['data']}

        delivery_deets.append(pickup_dict)

    # print(delivery_deets)
    return delivery_deets

# Create the actual pickup task if none exists, else update the existing task
def create_pickup_task(delivery_details, assigned):
    workerId = assigned
    url = "https://onfleet.com/api/v2/tasks"
    addy = get_hub_address()

    delivery_deets = delivery_details

    # Check to see if a pickup task already exists, and if so, update rather than create
    # TO DO: Should also be Assigned (not Active)
    # TO DO: Should update to support cases where new pickup task doesn't default to index = 0
    first_task_id = get_worker_tasks(workerId, 0)
    is_pickup = pickup_check(first_task_id)

    # If there's no pickup task, then we need to create one.
    # Address is the Hub.
    if is_pickup is False:

        payload = json.dumps({"destination": {"address": addy}, "recipients": [], "container": {"type": "WORKER",
                            "worker": workerId}, "pickupTask": "true", "metadata": [{"name": "Order Number","type":
                            "array", "subtype": "string", "value": [], "visibility": ["api", "dashboard"]}]})
        headers = {
            'Authorization': 'Basic ' + utilities.encode_b64(api_key),
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        new_task_id = (json.loads(response.text))['id']
        url2 = "https://onfleet.com/api/v2/containers/workers/" + workerId
        payload2 = json.dumps({"tasks": [0, new_task_id]})

        headers2 = {
            'Authorization': 'Basic ' + utilities.encode_b64(api_key),
            'Content-Type': 'text/plain'
        }

        requests.request("PUT", url2, headers=headers2, data=payload2)

        barcode_set = []
        order_number_set = []

        for d in delivery_deets:
            barcode = d['barcode']
            order_number = d['order_number']

            barcode_set.append({"data": barcode, "blockCompletion": True})
            order_number_set.append(order_number)

        order_numbers_for_pickup = order_number_set
        barcodes_for_pickup = barcode_set

        pickup_task = get_worker_tasks(workerId, 0)

        url = "https://onfleet.com/api/v2/tasks/" + pickup_task

        payload = json.dumps(
            {"metadata": {"$set": [{"name": "Order Number", "type": "array", "subtype": "string",
                                    "value": order_numbers_for_pickup, "visibility": ["api", "dashboard"]}]},
             "barcodes": barcodes_for_pickup}
        )
        # print(payload)
        headers = {
            'Authorization': 'Basic ' + utilities.encode_b64(api_key),
            'Content-Type': 'application/json'
        }

        requests.request("PUT", url, headers=headers, data=payload)

        for order in order_numbers_for_pickup:
            q.update_order_with_pickup_id(order, pickup_task)

    else:
        barcode_set = []
        order_number_set = []

        for order in delivery_deets:
            barcode = order['barcode']
            order_number = order['order_number']
            barcode_set.append({"data": barcode, "blockCompletion": True})
            order_number_set.append(order_number)

        order_numbers_for_pickup = order_number_set
        barcodes_for_pickup = barcode_set

        pickup_task = get_worker_tasks(workerId, 0)

        url = "https://onfleet.com/api/v2/tasks/" + pickup_task

        payload = json.dumps(
            {"metadata": {"$set":[{"name": "Order Number","type": "array", "subtype": "string",
            "value": order_numbers_for_pickup, "visibility": ["api", "dashboard"]}]},"barcodes": barcodes_for_pickup}
        )
        # print(payload)
        headers = {
            'Authorization': 'Basic ' + utilities.encode_b64(api_key),
            'Content-Type': 'application/json'
        }

        requests.request("PUT", url, headers=headers, data=payload)
        # print(response.text)
        print("Pickup task updated: " + pickup_task)

        for order in order_numbers_for_pickup:
            q.update_order_with_pickup_id(order, pickup_task)

    return






