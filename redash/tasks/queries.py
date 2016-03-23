import json
import time
import logging
import signal
import redis
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from redash import redis_connection, models, statsd_client, settings, utils
from redash.utils import gen_query_hash
from redash.worker import celery
from redash.query_runner import InterruptException
from .base import BaseTask
from .alerts import check_alerts_for_query

logger = get_task_logger(__name__)


def _job_lock_id(query_hash, data_source_id):
    return "query_hash_job:%s:%s" % (data_source_id, query_hash)


# TODO:
# There is some duplication between this class and QueryTask, but I wanted to implement the monitoring features without
# much changes to the existing code, so ended up creating another object. In the future we can merge them.
class QueryTaskTracker(object):
    DONE_LIST = 'query_task_trackers:done'
    WAITING_LIST = 'query_task_trackers:waiting'
    IN_PROGRESS_LIST = 'query_task_trackers:in_progress'
    ALL_LISTS = (DONE_LIST, WAITING_LIST, IN_PROGRESS_LIST)

    def __init__(self, data):
        self.data = data

    @classmethod
    def create(cls, task_id, state, query_hash, data_source_id, scheduled, metadata):
        data = dict(task_id=task_id, state=state,
                    query_hash=query_hash, data_source_id=data_source_id,
                    scheduled=scheduled,
                    username=metadata.get('Username', 'unknown'),
                    query_id=metadata.get('Query ID', 'unknown'),
                    retries=0,
                    scheduled_retries=0,
                    created_at=time.time(),
                    started_at=None,
                    run_time=None)

        return cls(data)

    def save(self, connection=None):
        if connection is None:
            connection = redis_connection

        self.data['updated_at'] = time.time()
        key_name = self._key_name(self.data['task_id'])
        connection.set(key_name, utils.json_dumps(self.data))
        connection.zadd('query_task_trackers', time.time(), key_name)

        connection.zadd(self._get_list(), time.time(), key_name)

        for l in self.ALL_LISTS:
            if l != self._get_list():
                connection.zrem(l, key_name)

    def update(self, **kwargs):
        self.data.update(kwargs)
        self.save()

    @staticmethod
    def _key_name(task_id):
        return 'query_task_tracker:{}'.format(task_id)

    def _get_list(self):
        if self.state in ('finished', 'failed'):
            return self.DONE_LIST

        if self.state in ('created'):
            return self.WAITING_LIST

        return self.IN_PROGRESS_LIST

    @classmethod
    def get_by_task_id(cls, task_id, connection=None):
        if connection is None:
            connection = redis_connection

        key_name = cls._key_name(task_id)
        data = connection.get(key_name)
        return cls.create_from_data(data)

    @classmethod
    def create_from_data(cls, data):
        if data:
            data = json.loads(data)
            return cls(data)

        return None

    @classmethod
    def all(cls, list_name, offset=0, limit=-1):
        if limit != -1:
            limit -= 1

        if offset != 0:
            offset -= 1

        ids = redis_connection.zrevrange(list_name, offset, limit)
        pipe = redis_connection.pipeline()
        for id in ids:
            pipe.get(id)

        tasks = [cls.create_from_data(data) for data in pipe.execute()]
        return tasks

    def __getattr__(self, item):
        return self.data[item]

    def __contains__(self, item):
        return item in self.data


class QueryTask(object):
    # TODO: this is mapping to the old Job class statuses. Need to update the client side and remove this
    STATUSES = {
        'PENDING': 1,
        'STARTED': 2,
        'SUCCESS': 3,
        'FAILURE': 4,
        'REVOKED': 4
    }

    def __init__(self, job_id=None, async_result=None):
        if async_result:
            self._async_result = async_result
        else:
            self._async_result = AsyncResult(job_id, app=celery)

    @property
    def id(self):
        return self._async_result.id

    def to_dict(self):
        if self._async_result.status == 'STARTED':
            updated_at = self._async_result.result.get('start_time', 0)
        else:
            updated_at = 0

        status = self.STATUSES[self._async_result.status]

        if isinstance(self._async_result.result, Exception):
            error = self._async_result.result.message
            status = 4
        elif self._async_result.status == 'REVOKED':
            error = 'Query execution cancelled.'
        else:
            error = ''

        if self._async_result.successful() and not error:
            query_result_id = self._async_result.result
        else:
            query_result_id = None

        return {
            'id': self._async_result.id,
            'updated_at': updated_at,
            'status': status,
            'error': error,
            'query_result_id': query_result_id,
        }

    @property
    def is_cancelled(self):
        return self._async_result.status == 'REVOKED'

    @property
    def celery_status(self):
        return self._async_result.status

    def ready(self):
        return self._async_result.ready()

    def cancel(self):
        return self._async_result.revoke(terminate=True, signal='SIGINT')


def enqueue_query(query, data_source, scheduled=False, metadata={}):
    query_hash = gen_query_hash(query)
    logging.info("Inserting job for %s with metadata=%s", query_hash, metadata)
    try_count = 0
    job = None

    while try_count < 5:
        try_count += 1

        pipe = redis_connection.pipeline()
        try:
            pipe.watch(_job_lock_id(query_hash, data_source.id))
            job_id = pipe.get(_job_lock_id(query_hash, data_source.id))
            if job_id:
                logging.info("[%s] Found existing job: %s", query_hash, job_id)

                job = QueryTask(job_id=job_id)
                tracker = QueryTaskTracker.get_by_task_id(job_id, connection=pipe)
                # tracker might not exist, if it's an old job
                if scheduled and tracker:
                    tracker.update(retries=tracker.retries+1)
                elif tracker:
                    tracker.update(scheduled_retries=tracker.scheduled_retries+1)

                if job.ready():
                    logging.info("[%s] job found is ready (%s), removing lock", query_hash, job.celery_status)
                    redis_connection.delete(_job_lock_id(query_hash, data_source.id))
                    job = None

            if not job:
                pipe.multi()

                if scheduled:
                    queue_name = data_source.scheduled_queue_name
                else:
                    queue_name = data_source.queue_name

                result = execute_query.apply_async(args=(query, data_source.id, metadata), queue=queue_name)
                job = QueryTask(async_result=result)
                tracker = QueryTaskTracker.create(result.id, 'created', query_hash, data_source.id, scheduled, metadata)
                tracker.save(connection=pipe)

                logging.info("[%s] Created new job: %s", query_hash, job.id)
                pipe.set(_job_lock_id(query_hash, data_source.id), job.id, settings.JOB_EXPIRY_TIME)
                pipe.execute()
            break

        except redis.WatchError:
            continue

    if not job:
        logging.error("[Manager][%s] Failed adding job for query.", query_hash)

    return job


@celery.task(name="redash.tasks.refresh_queries", base=BaseTask)
def refresh_queries():
    logger.info("Refreshing queries...")

    outdated_queries_count = 0
    query_ids = []

    with statsd_client.timer('manager.outdated_queries_lookup'):
        for query in models.Query.outdated_queries():
            enqueue_query(query.query, query.data_source,
                          scheduled=True,
                          metadata={'Query ID': query.id, 'Username': 'Scheduled'})
            query_ids.append(query.id)
            outdated_queries_count += 1

    statsd_client.gauge('manager.outdated_queries', outdated_queries_count)

    logger.info("Done refreshing queries. Found %d outdated queries: %s" % (outdated_queries_count, query_ids))

    status = redis_connection.hgetall('redash:status')
    now = time.time()

    redis_connection.hmset('redash:status', {
        'outdated_queries_count': outdated_queries_count,
        'last_refresh_at': now,
        'query_ids': json.dumps(query_ids)
    })

    statsd_client.gauge('manager.seconds_since_refresh', now - float(status.get('last_refresh_at', now)))


@celery.task(name="redash.tasks.cleanup_tasks", base=BaseTask)
def cleanup_tasks():
    # in case of cold restart of the workers, there might be jobs that still have their "lock" object, but aren't really
    # going to run. this job removes them.
    lock_keys = redis_connection.keys("query_hash_job:*") # TODO: use set instead of keys command
    if not lock_keys:
        return

    query_tasks = [QueryTask(job_id=j) for j in redis_connection.mget(lock_keys)]

    logger.info("Found %d locks", len(query_tasks))

    inspect = celery.control.inspect()
    active_tasks = inspect.active()
    if active_tasks is None:
        active_tasks = []
    else:
        active_tasks = active_tasks.values()

    all_tasks = set()
    for task_list in active_tasks:
        for task in task_list:
            all_tasks.add(task['id'])

    logger.info("Active jobs count: %d", len(all_tasks))

    for i, t in enumerate(query_tasks):
        if t.ready():
            # if locked task is ready already (failed, finished, revoked), we don't need the lock anymore
            logger.warning("%s is ready (%s), removing lock.", lock_keys[i], t.celery_status)
            redis_connection.delete(lock_keys[i])


@celery.task(name="redash.tasks.cleanup_query_results", base=BaseTask)
def cleanup_query_results():
    """
    Job to cleanup unused query results -- such that no query links to them anymore, and older than
    settings.QUERY_RESULTS_MAX_AGE (a week by default, so it's less likely to be open in someone's browser and be used).

    Each time the job deletes only settings.QUERY_RESULTS_CLEANUP_COUNT (100 by default) query results so it won't choke
    the database in case of many such results.
    """

    logging.info("Running query results clean up (removing maximum of %d unused results, that are %d days old or more)",
                 settings.QUERY_RESULTS_CLEANUP_COUNT, settings.QUERY_RESULTS_CLEANUP_MAX_AGE)

    unused_query_results = models.QueryResult.unused(settings.QUERY_RESULTS_CLEANUP_MAX_AGE).limit(settings.QUERY_RESULTS_CLEANUP_COUNT)
    total_unused_query_results = models.QueryResult.unused().count()
    deleted_count = models.QueryResult.delete().where(models.QueryResult.id << unused_query_results).execute()

    logger.info("Deleted %d unused query results out of total of %d." % (deleted_count, total_unused_query_results))


@celery.task(name="redash.tasks.refresh_schemas", base=BaseTask)
def refresh_schemas():
    """
    Refreshes the data sources schemas.
    """
    for ds in models.DataSource.select():
        logger.info("Refreshing schema for: {}".format(ds.name))
        try:
            ds.get_schema(refresh=True)
        except Exception:
            logger.exception("Failed refreshing the data source: %s", ds.name)


def signal_handler(*args):
    raise InterruptException


class QueryExecutionError(Exception):
    pass


# We could have created this as a celery.Task derived class, and act as the task itself. But this might result in weird
# issues as the task class created once per process, so decided to have a plain object instead.
class QueryExecutor(object):
    def __init__(self, task, query, data_source_id, metadata):
        self.task = task
        self.query = query
        self.data_source_id = data_source_id
        self.metadata = metadata
        self.data_source = self._load_data_source()
        self.query_hash = gen_query_hash(self.query)
        # Load existing tracker or create a new one if the job was created before code update:
        self.tracker = QueryTaskTracker.get_by_task_id(task.request.id) or QueryTaskTracker.create(task.request.id,
                                                                                                   'created',
                                                                                                   self.query_hash,
                                                                                                   self.data_source_id,
                                                                                                   False, metadata)

    def run(self):
        signal.signal(signal.SIGINT, signal_handler)
        self.tracker.update(started_at=time.time(), state='started')

        logger.debug("Executing query:\n%s", self.query)
        self._log_progress('executing_query')

        query_runner = self.data_source.query_runner
        annotated_query = self._annotate_query(query_runner)
        data, error = query_runner.run_query(annotated_query)
        run_time = time.time() - self.tracker.started_at
        self.tracker.update(error=error, run_time=run_time, state='saving_results')

        logger.info("task=execute_query query_hash=%s data_length=%s error=[%s]", self.query_hash, data and len(data), error)

        redis_connection.delete(_job_lock_id(self.query_hash, self.data_source.id))

        if error:
            self.tracker.update(state='failed')
            result = QueryExecutionError(error)
        else:
            query_result, updated_query_ids = models.QueryResult.store_result(self.data_source.org_id, self.data_source.id,
                                                                              self.query_hash, self.query, data,
                                                                              run_time, utils.utcnow())
            self._log_progress('checking_alerts')
            for query_id in updated_query_ids:
                check_alerts_for_query.delay(query_id)
            self._log_progress('finished')

            result = query_result.id

        return result

    def _annotate_query(self, query_runner):
        if query_runner.annotate_query():
            self.metadata['Task ID'] = self.task.request.id
            self.metadata['Query Hash'] = self.query_hash
            self.metadata['Queue'] = self.task.request.delivery_info['routing_key']

            annotation = u", ".join([u"{}: {}".format(k, v) for k, v in self.metadata.iteritems()])
            annotated_query = u"/* {} */ {}".format(annotation, self.query)
        else:
            annotated_query = self.query
        return annotated_query

    def _log_progress(self, state):
        logger.info("task=execute_query state=%s query_hash=%s type=%s ds_id=%d task_id=%s queue=%s query_id=%s username=%s",
                    state,
                    self.query_hash, self.data_source.type, self.data_source.id, self.task.request.id, self.task.request.delivery_info['routing_key'],
                    self.metadata.get('Query ID', 'unknown'), self.metadata.get('Username', 'unknown'))
        self.tracker.update(state=state)

    def _load_data_source(self):
        logger.info("task=execute_query state=load_ds ds_id=%d", self.data_source_id)
        return models.DataSource.get_by_id(self.data_source_id)


@celery.task(name="redash.tasks.execute_query", bind=True, base=BaseTask, track_started=True)
def execute_query(self, query, data_source_id, metadata):
    return QueryExecutor(self, query, data_source_id, metadata).run()
