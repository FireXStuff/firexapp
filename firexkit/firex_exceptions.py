import subprocess
from signal import Signals


def shorten_long_output(output, max_output_str_len=8000):
    if len(output) > max_output_str_len:
        mssg = f'\nOutput (last {max_output_str_len} chars):\n{output[-max_output_str_len:]}'
    else:
        mssg = f'\nOutput:\n{output}'
    return mssg


class FireXCalledProcessError(subprocess.CalledProcessError):
    def __str__(self):
        if self.returncode < 0:
            # POSIX says this is a signal
            try:
                signame = Signals(-self.returncode).name
            except ValueError:
                signame = -self.returncode

            status = f'signal {signame}'
        else:
            status = f'exit code {self.returncode}'

        mssg = f'Command {self.cmd} exited with {status}.'
        if self.output:
            mssg += shorten_long_output(self.output)
        return mssg


class FireXInactivityTimeoutExpired(subprocess.TimeoutExpired):
    # When instantiating the exception, make sure you provide the necessary positional args as args, not kwargs.
    def __str__(self):
        mssg = (
            f"Command '{self.cmd}' timed out because it did not generate any console output "
            f"for more than {int(self.timeout)} seconds."
        )
        if self.output:
            mssg += shorten_long_output(self.output)
        return mssg
