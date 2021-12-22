from flask import Flask, request, make_response
import os
import json
import queryDB as q
import API_calls as c
import utilities
app = Flask(__name__)

def verify_webhook(header, body, secret):
    """Verifies that the webhook originated from Onfleet.
    Args:
        header: value of the X-Onfleet-Signature header in raw bytes
        body: should be the full body of the POST request in raw bytes, *not* the parsed JSON object
        secret: the value of the webhook secret from the Onfleet dashboard, in hexadecimal format
    Returns:
        True for verified, False for not verified"""
    import hashlib, hmac, binascii
    return hmac.new(binascii.a2b_hex(secret), body, 'sha512').hexdigest() == header

def warn_if_unverified(req):
  if app.secret and not verify_webhook(req.headers['X-Onfleet-Signature'], req.data, app.secret):
        print("Warning: could not verify the origin of the webhook invocation with provided secret key.")


@app.before_first_request
def check_for_secret():
  if "WEBHOOK_SECRET" in os.environ:
    app.secret = os.environ["WEBHOOK_SECRET"]
  else:
    print("\n***  NOTICE: Webhooks are running in unverified mode.  ***")
    print("To verify webhooks, copy your Webhook secret key from the Onfleet dashboard "
    "and set it in the environment as WEBHOOK_SECRET before running this script.\n\n"
    "For example, export FLASK_APP=testwebhooks.py WEBHOOK_SECRET=XXXXXXXXXXXXXXXXXXXXX flask run")
    app.secret = None


@app.route('/taskassigned_create_pickup', methods=['GET','POST'])
def taskassigned():
  # Validate call is from Onfleet
  if request.method == 'GET':
    return request.args.get('check','')
  elif request.method == 'POST':
    warn_if_unverified(request)

    response = request.get_json()

    pickup_check = response['data']['task']['pickupTask']

    if pickup_check is False:
      taskid = response['taskId']
      workerid = response['workerId']

      q.insert_assigned_task(taskid, workerid)

      return '', 200

    else:
      print("Pickup task")

      return '', 200



