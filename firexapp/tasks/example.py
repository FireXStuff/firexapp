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
    greetings = []
    for guest in guests:
        # Create a Celery Signature, see: https://docs.celeryproject.org/en/latest/userguide/canvas.html#signatures
        greet_signature = greet.s(name=guest)
        # enqueue_child_and_get_results is useful when you want to block on a service invocation and receive the result
        # immediately. See the details of other enqueue methods here: ____
        greet_results = self.enqueue_child_and_get_results(greet_signature)
        greetings.append(greet_results['greeting'])

    return ' '.join(greetings)


@InputConverter.register
@SingleArgDecorator('guests')
def to_list(guests):
    return guests.split(',')
