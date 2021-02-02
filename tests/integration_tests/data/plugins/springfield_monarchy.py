from firexkit.chain import InjectArgs
from firexkit.task import FireXTask
from firexkit.chain import returns

from firexapp.engine.celery import app


@app.task(bind=True)
@returns('job_title', FireXTask.DYNAMIC_RETURN)
def get_springfield_power_plant_job_title(self: FireXTask):
    title_to_monarch = {'OWNER': 'KING',
                        'EXECUTIVE ASSISTANT': 'PRINCE',
                        'DIRECTOR': 'DUKE',
                        'SUPERVISOR': 'CHANCELLOR'}

    # Invoke the original version of the service with all arguments available to to this service: self.abog
    chain = InjectArgs(**self.abog) | self.orig.s()
    orig_ret = self.enqueue_child_and_get_results(chain)

    # Extract the job title from the original results, removing it from the orig_ret dict.
    orig_job_title = orig_ret.pop('job_title')
    # Map the traditional job title to its monarchy equivalent.
    monarchy_job_title = title_to_monarch.get(orig_job_title, 'PEASANT')

    # Return the monarchy title + anything else returned by the original service.
    return monarchy_job_title, orig_ret
