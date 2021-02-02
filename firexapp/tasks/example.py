from getpass import getuser
import time

from firexkit.argument_conversion import SingleArgDecorator
from firexkit.chain import InjectArgs, returns
from firexkit.task import FireXTask, flame, flame_collapse

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
@app.task(returns=['greeting'], flame=['greeting'])
def greet(name=getuser()):
    assert len(name) > 1, "Cannot greet a name with 1 or fewer characters."
    return 'Hello %s!' % name


# Setting bind=True makes the first argument received by the service 'self'. It's most commonly used to invoke
# (enqueue) other services, but provides much more functionality as outlined here:
@app.task(bind=True, returns=['guests_greeting'], flame=['guests_greeting'])
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
def amplify(to_amplify, upper=True, surround_str=None, underline_char=None, overline_char=None):
    result = to_amplify
    if upper:
        result = to_amplify.upper()
    if surround_str:
        result = surround_str + result + surround_str
    centerline_len = len(result)
    if underline_char:
        result = result + '\n' + (underline_char * centerline_len)

    if overline_char:
        result = (overline_char * centerline_len) + '\n' + result

    return result


def _amplified_greeting_formatter(args_and_maybe_results):
    # Since 'amplified_greeting' is the return value name, it isn't available to the formatter when the task is first
    # started. It will be available if the task completes successfully.
    if 'amplified_greeting' in args_and_maybe_results:
        br_as_nl = args_and_maybe_results["amplified_greeting"].replace('\n', '<br>')
        return f'<h1 style="font-family: monospace;">{br_as_nl}</h1>'

    # Since 'guests' is an input argument, it will always be available to the formatter, even before the service
    # has completed.
    return f'Planning to greet: {",".join(args_and_maybe_results["guests"])}'


@app.task(bind=True, returns=['amplified_greeting'])
@flame('*', _amplified_greeting_formatter)
@flame_collapse({'greet_guests': 'descendants'})
def amplified_greet_guests(self: FireXTask, guests):
    # Nonsense failure case to illustrate flame HTML data when the service fails (i.e. no return value present).
    assert len(guests) > 1, "Only willing to amplify greeting for more than one guest."

    # Create a chain that can be enqueued. The greet_guests service will produce a guests_greeting,
    # which will then be delivered to amplify as its to_amplify argument.
    amplified_greet_guests_chain = InjectArgs(**self.abog) | greet_guests.s() | amplify.s(to_amplify='@guests_greeting')

    # Chains can be enqueued just like signatures. You can consider a signature a chain with only one service.
    chain_results = self.enqueue_child_and_get_results(amplified_greet_guests_chain)
    return chain_results['amplified_message']


@app.task()
@returns('job_title')
def get_springfield_power_plant_job_title(name):
    username_to_title = {'Charles Montgomery Burns': 'OWNER',
                         'Waylon Smithers': 'EXECUTIVE ASSISTANT',
                         'Lenny Leonard': 'DIRECTOR',
                         'Homer Simpson': 'SUPERVISOR'}
    return username_to_title.get(name, 'UNKNOWN')


@InputConverter.register
@SingleArgDecorator('employee_names')
def employee_names_to_list(employee_names):
    return employee_names.split(',')


@app.task(bind=True, returns=['amplified_greeting'])
def greet_springfield_power_plant_employees(self, employee_names):
    names_with_titles = []
    for name in employee_names:
        job_title = self.enqueue_child_and_get_results(get_springfield_power_plant_job_title.s(name=name))['job_title']
        names_with_titles.append(f"{job_title} {name}")

    results = self.enqueue_child_and_get_results(amplified_greet_guests.s(guests=names_with_titles))
    return results['amplified_greeting']
