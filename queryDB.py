from sqlalchemy import create_engine, text

#Classes
class DBConnect(object):

    def __init__(self, db, query, *args):
        self.database = db
        self.connectionString = "postgresql://localhost/"
        self.text = query
        self.params = []

        for arg in args:
            self.params.append(arg)


    def __enter__(self):
            if not self.params:
                self.execute = create_engine(self.connectionString + self.database).connect().execute(text(self.text))
            else:
                self.execute = create_engine(self.connectionString + self.database).connect().execute(text(self.text),
                                        {"x": self.params[0], "y": self.params[1]})
            return self.execute

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


database = "sethlipman"

#Functions
def new_orders():
    new_order_list = []
    query = "select * from vw_neworder"

    with DBConnect(database, query) as conn:
        orders = conn
        for r in orders:
            new_order_list.append(dict(r))

        print(new_order_list)
    return new_order_list


def update_order_with_task_id(tasks=None):
    if not tasks:
        print("No new orders.")
        return

    else:
        query = "UPDATE tbl_order SET onfleet_delivery_task_id=:x WHERE order_number=:y"

        i = 0
        while i < len(tasks):
            task_order_dict = tasks[i]

            taskid = task_order_dict['taskId']
            order_number = task_order_dict['order_number']

            with DBConnect(database, query, taskid, order_number):

                i = i + 1

        return


def insert_assigned_task(taskid=None, workerid=None):
    query = "INSERT INTO tbl_assigned_tasks (taskid, workerid) VALUES (:x, :y)"

    with DBConnect(database, query, taskid, workerid):
        return


def get_workers_unprocessed_tasks():
    assigned = []
    query = "UPDATE tbl_assigned_tasks SET processing_status = 'PROCESSING' WHERE processing_status = 'UNPROCESSED'"

    with DBConnect(database, query):
        try:
            pass
        except:
            print("exception with unprocessed tasks.")

    query2 = "SELECT DISTINCT workerid FROM tbl_assigned_tasks where processing_status = 'PROCESSING'"

    with DBConnect(database, query2) as tasks:
        try:
            # Update status to avoid double dipping
            for r in tasks:
                assigned.append(dict(r))

        except:
            print("Exception with processing tasks.")

    print(assigned)
    return assigned

def update_task_processed():
    query = "UPDATE tbl_assigned_tasks SET processing_status = 'PROCESSED' WHERE processing_status = 'PROCESSING'"

    with DBConnect(database, query):
        try:
            pass
        except:
            print("exception with processing task.")
            return


def update_order_with_pickup_id(orderid, taskid):
    query = "UPDATE tbl_order SET onfleet_pickup_task_id = :x WHERE order_number = :y"

    with DBConnect(database, query, taskid, orderid):
        try:
            pass
        except:
            print("exception with pickupid update.")
            return