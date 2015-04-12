# -*- coding: utf-8 -*-
from datetime import datetime
from decimal import Decimal
import time
import traceback

from functools import wraps
from tendo import singleton
from django.conf import settings

from utils.storage import get_current_redis
from utils.mail import send_error


def cron_stats_decorator(redis_key, cron_max_no_job_minutes=0, cron_min_no_job_minutes=0,
                         cron_max_job_time=settings.CRON_MAX_JOB_TIME):
    """
    Декоратор для management commands:
        отмечает факт начала выполнения,
        контролирует частоту вызовов,
        подсчитывает статистику вызовов,
        отсылает письмо в случае исключения или слишком долгого выполнения команды

    :param redis_key: management command unique key
    """
    tmdelta = datetime.today() - datetime.utcfromtimestamp(0)
    cron_redis_key = ''.join([settings.CRON_STATS_PREFIX, str(tmdelta.days), '__', redis_key])

    def actual(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            client = get_current_redis()

            job_key = "_".join([settings.CRON_JOB_PREFIX, redis_key])

            # check called not more often than we need
            if cron_min_no_job_minutes:
                job = client.hgetall(job_key)
                finish_time = job.get('finish_time', False)

                time_error = ''
                if not finish_time:
                    time_error = 'Warning: no finish time provided for %s. (Okay when called first time)' % job_key
                else:
                    delta = (start - float(finish_time)) / 60
                    if delta < cron_min_no_job_minutes:
                        time_error = ("Command is being called too often. <br> Redis key: %s <br> "
                                      "Start time difference: %s minutes" % (redis_key, str(delta)))

                if time_error:
                    send_error('cron_strat_error_%s' % redis_key, subject='Command start time warning.',
                               message=time_error)

            # remember start_time - is checked by 'check_cron_jobs' command
            # remember cron_max_no_job_minutes - for detecting jobs, which haven't been launched for a long time
            job_start_hkey = "_".join(["start_time", str(int(start))])
            client.hmset(job_key, {
                job_start_hkey: start,
                settings.CRON_MAX_JOB_TIME_HKEY: cron_max_job_time,
                settings.CRON_MAX_NO_JOB_TIME_HKEY: cron_max_no_job_minutes * 60,
            })

            try:
                res = func(*args, **kwargs)
            except Exception as e:
                import inspect
                client.hincrby(cron_redis_key, 'failures')
                message = "ERROR AT: %s.%s\n MESSAGE: %s" % (inspect.getmodule(func).__name__, func.__name__, e)
                send_error('management_error_%s' % redis_key, subject="Management command error", message=message, cache_timeout=60 * 9)
                print 'Management command error:\n%s' % message
                traceback.print_exc()
                return
            finally:
                # write statistics
                finish = time.time()
                delta = _write_stats_to_redis(cron_redis_key, start, client=client)
                # the command succeded or failed but finished - delete start key
                if int(delta) > cron_max_job_time:
                    send_error(
                        'long_cron_finished_%s' % redis_key,
                        subject='Cron job finished but took too much time.',
                        message="Redis key: %s <br> Execution time: %s seconds <br> cron_max_job_time == %s seconds" % (
                            redis_key, str(delta), cron_max_job_time
                        )
                    )
                client.hmset(job_key, {"finish_time": finish})
                client.hdel(job_key, job_start_hkey)

            return res
        return wrapper
    return actual


def _write_stats_to_redis(redis_key, start, client=None):
    """
    Логика подсчета статистики вызовов и записи в редис
    """
    client = client or get_current_redis()
    end = time.time()
    delta = end - start
    profiling_data = client.hgetall(redis_key)
    maximum = Decimal(profiling_data.get('max', 0))
    avg = float(profiling_data.get('avg', 0))
    num = int(profiling_data.get('num', 0))
    new_num = num + 1
    new_max = profiling_data['max'] if maximum > Decimal.from_float(delta) else delta
    new_avg = (avg * num + delta) / new_num
    client.hmset(redis_key, {'max': new_max, 'num': new_num, 'avg': new_avg})
    return delta


def singleton_script_decorator(lock_name=""):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            flavor_id = lock_name or func.__name__
            singleton.SingleInstance(flavor_id=flavor_id)
            return func(*args, **kwargs)
        return wrapper
    return decorator