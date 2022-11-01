import scheduler

from functools import partial, update_wrapper
from datetime import datetime, timedelta, time
from time import sleep

from utils import base_errors


class SchedulerError(BaseException):
    pass


class SchedulerValueError(SchedulerError):
    pass


class IntervalError(SchedulerError):
    pass


class Scheduler:
    def __init__(self):
        self.jobs = list()

    def every(self, interval: int = 1):
        job = Job(interval)
        self.jobs.append(job)
        return job

    def run_pending(self):
        all_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(all_jobs):
            job.run()

    def run_all(self, delay_second: int = 0):
        for job in self.jobs:
            job.run()
            sleep(delay_second)

    @property
    def next_run(self):
        if not self.jobs:
            return None
        return min(self.jobs).next_run

    @property
    def idle_seconds(self):
        return self.next_run - datetime.now()


class Job:
    def __init__(self, interval: int):
        self.interval = interval
        self.period = None
        self.unit = None
        self.job_func = None
        self.last_run = None
        self.next_run = None
        self.at_time = None

    def __lt__(self, other):
        return self.next_run <= other.next_run

    @property
    def second(self):
        if self.interval != 1:
            raise IntervalError(base_errors.interval_error('second'))
        return self.seconds

    @property
    def seconds(self):
        self.unit = 'seconds'
        return self

    @property
    def minute(self):
        if self.interval != 1:
            raise IntervalError(base_errors.interval_error('minute'))
        return self.minutes

    @property
    def minutes(self):
        self.unit = 'minutes'
        return self

    @property
    def hour(self):
        if self.interval != 1:
            raise IntervalError(base_errors.interval_error('hour'))
        return self.hours

    @property
    def hours(self):
        self.unit = 'hours'
        return self

    @property
    def day(self):
        if self.interval != 1:
            raise IntervalError(base_errors.interval_error('day'))
        return self.days

    @property
    def days(self):
        self.unit = 'days'
        return self

    @property
    def week(self):
        if self.interval != 1:
            raise IntervalError(base_errors.interval_error('week'))
        return self.weeks

    @property
    def weeks(self):
        self.unit = 'weeks'
        return self

    @property
    def should_run(self):
        return self.next_run <= datetime.now()

    def do(self, job_func, *args, **kwargs):
        self.job_func = partial(job_func, *args, **kwargs)
        update_wrapper(self.job_func, job_func)
        self._schedule_next_run()
        return self

    def _schedule_next_run(self):
        if self.unit not in ('seconds', 'minutes', 'hours', 'days',
                             'weeks'):
            raise SchedulerValueError(base_errors.scheduler_value_error())

        self.period = timedelta(**{self.unit: self.interval})

        self.next_run = datetime.now() + self.period

        if self.at_time:
            if self.unit not in ('hours', 'days'):
                raise SchedulerValueError(base_errors.scheduler_value_error())
            kwargs = {
                'minute': self.at_time.minute,
                'second': 0,
                'microsecond': 0,
            }
            if self.unit == 'days':
                kwargs['hours'] = self.at_time.hour

            self.next_run = self.next_run.replace(**kwargs)

            if not self.last_run: #for first run
                now = datetime.now()
                if self.unit == 'days' and self.at_time > now.time():
                    self.next_run = self.next_run - timedelta(days=1)
                elif self.unit == 'hours' and self.at_time.minute > now.minute:
                    self.next_run = self.next_run - timedelta(hours=1)

    def run(self):
        ret = self.job_func()
        self.last_run = datetime.now()
        self._schedule_next_run()
        return ret

    def at(self, time_value: str):
        if self.unit not in ('hours', 'days'):
            raise SchedulerValueError(base_errors.scheduler_value_error())
        hour, minute = [t for t in time_value.split(':')]

        if self.unit == 'days':
            hour = int(hour)
            if not 0 <= hour <= 23:
                raise SchedulerValueError(base_errors.hour_wrong)
        elif self.unit == 'hours':
            hour = 0

        minute = int(minute)
        if not 0 <= minute <= 59:
            raise SchedulerValueError(base_errors.minute_wrong)

        self.at_time = time(
            hour=hour,
            minute=minute,
        )
        return self


default_scheduler = Scheduler()


def every(interval: int = 1):
    return default_scheduler.every(interval)


def run_pending():
    return default_scheduler.run_pending()


def run_all(delay_second: int = 0):
    return default_scheduler.run_all(delay_second)


def next_run():
    return default_scheduler.next_run


def idle_seconds():
    return default_scheduler.idle_seconds

