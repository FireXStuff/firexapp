from getpass import getuser
import time

from firexkit.argument_conversion import SingleArgDecorator
from firexkit.task import FireXTask

from firexapp.engine.celery import app
from firexapp.submit.arguments import InputConverter


@app.task
def nop():
    return


@app.task
def sleep(sleep=None):
    if sleep:
        time.sleep(int(sleep))
    return


@app.task(returns='username')
def getusername():
    return getuser()


# The @app.task() makes this normal python function a FireX Service.
@app.task(returns=['greeting'])
def greet(name=getuser()):
    return 'Hello %s!' % name


# Setting bind=True makes the first argument received by the service 'self'. It's most commonly used to invoke
# (enqueue) other services, but provides much more functionality as outlined here:
@app.task(bind=True, returns=['guests_greeting'])
def greet_guests(self: FireXTask, guests):
    child_promises = []
    for guest in guests:
        # Create a Celery Signature, see: https://docs.celeryproject.org/en/latest/userguide/canvas.html#signatures
        greet_signature = greet.s(name=guest)
        # enqueue_child by default schedules the supplied signature for execution asynchronously and immediately
        # returns the newly created child result promise.
        child_promise = self.enqueue_child(greet_signature)
        child_promises.append(child_promise)

    # We want to get all the return values (greetings) from child tasks, but we must wait first to make sure they're all
    # available before inspecting them with child_promise.result[<returns_key>]
    self.wait_for_children()
    # Since wait_for_children has completed, we know it's safe to inspect the results of all child task promises.
    greetings = [promise.result['greeting'] for promise in child_promises]
    return ' '.join(greetings)


@InputConverter.register
@SingleArgDecorator('guests')
def to_list(guests):
    return guests.split(',')
