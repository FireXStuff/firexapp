from firexapp.submit.submit import SubmitBaseApp


# noinspection PyUnusedLocal
def just_barf_instead(self, **kwargs):
    raise Exception("Fail for test")


SubmitBaseApp.start_celery = just_barf_instead
