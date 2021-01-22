from getpass import getuser
import time

from firexapp.engine.celery import app


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
