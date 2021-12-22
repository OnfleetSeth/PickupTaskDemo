import queryDB as q
import API_calls as c
import utilities
import time

# Executing this program will demonstrate:
# 1. Creating delivery tasks from records in a database
# 2. Using Onfleet Webhooks (taskAssigned) to trigger a workflow
# 3. Queueing messages from the Webhook integration (in this case - using Postgres)
# 4. Creating a pickup task for ASSIGNED delivery tasks OR
# 5. Updating a pickup task with details from newly assigned tasks
# 6. Database write back for both queue management, as well as task details

# Include this if we want to create tasks from the database. If tasks exist, no need.
# c.create_new_tasks()

def main():

    while True:
        try:
            assigned = q.get_workers_unprocessed_tasks()

            if not assigned:
                print("No unprocessed delivery tasks to pickup.")

            else:
                for workers in assigned:
                    workerid = workers['workerid']
                    print("Driver has new delivery tasks: " + workerid)

                    tasks = c.get_worker_tasks(workerid)
                    delivery_details = c.get_delivery_deets(tasks)
                    c.create_pickup_task(delivery_details, workerid)

            q.update_task_processed()
            time.sleep(10)

        except KeyboardInterrupt:
            print("Program stopped.")
            break

main()