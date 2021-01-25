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
    assert len(name) > 1, "Cannot greet a name with 1 or fewer characters."
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
    self.wait_for_children(raise_exception_on_failure=False)
    # Since wait_for_children has completed, we know it's safe to inspect the results of all child task promises,
    # after verifying the task was a success.
    greetings = [promise.result['greeting'] for promise in child_promises if promise.successful()]

    if any(promise.failed() for promise in child_promises):
        greetings.append("And apologies to those not mentioned.")

    return ' '.join(greetings)


@InputConverter.register
@SingleArgDecorator('guests')
def to_list(guests):
    return guests.split(',')


@app.task(returns=['amplified_message'])
def amplify(to_amplify):
    return to_amplify.upper()


@app.task(bind=True, returns=['amplified_greeting'])
def amplified_greet_guests(self: FireXTask, guests):

    # Create a chain that can be enqueued. The greet_guests service will produce a guests_greeting,
    # which will then be delivered to amplify as its to_amplify argument.
    amplified_greet_guests_chain = greet_guests.s(guests=guests) | amplify.s(to_amplify='@guests_greeting')

    # Chains can be enqueued just like signatures. You can consider a signature a chain with only one service.
    chain_results = self.enqueue_child_and_get_results(amplified_greet_guests_chain)
    return chain_results['amplified_message']
