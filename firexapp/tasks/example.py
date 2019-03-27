from firexapp.engine.celery import app
import time


@app.task()
def nop():
    return


@app.task()
def sleep(sleep=None):
    if sleep:
        time.sleep(int(sleep))
    return
