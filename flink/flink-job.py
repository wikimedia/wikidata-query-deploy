#!/usr/bin/env python3

# This script allows to manipulate the WDQS streaming Updater flink job running inside a flink session cluster.
# 3 environments are supported: eqiad, codfw and staging (which is likely to be running in eqiad).
# The configuration is sourced from multiple places:
# - /etc/helmfile-defaults/general-[eqiad|codfw|staging].yaml for the config owned by puppet
# - the file rdf-streaming-updater.yaml for the common values with a minimal templating system to inject the env
#   dependent values
# - OPTIONS_PER_ENV here for env specific setups
#
# The script can:
# - deploy a job from an optional initial-state
# - schedule a savepoint for a running savepoint
# - stop a job with a savepoint
# - status: see a quick status of the job
# - redeploy: all-in-one command for deploying a version upgrade: stop with savepoint + start from this savepoint
#
# The deploy strategy is heavily dependent on the name of the job, it assumes only one job with a particular name
# can be "running" at a given time.
#
# Note on path, the options requiring a path to the object store can be given a relative path, e.g. specifying
# "savepoints" for staging will prepend the corresponding entry in the OBJECT_STORAGE_BASE array. A full path can
# be given (starting with scheme:// e.g. s3://).
#
# Example usage:
# Deploying the version 0.3.77 of the job using the name "WDQS Streaming Updater" in eqiad starting from
# checkpoints/4b6ab3633baac70fd899f56f8d5797b2/chk-86950:
#   python3 flink/flink-job.py \
#       --env eqiad \
#       --job-name "WDQS Streaming Updater" \
#       deploy \
#       --jar lib/streaming-updater-producer-0.3.77-jar-with-dependencies.jar \
#       --options-file flink/rdf-streaming-updater.yaml \
#       --initial-state checkpoints/4b6ab3633baac70fd899f56f8d5797b2/chk-86950
#
# Upgrade the job to version 0.3.77 in codfw using savepoints as the base directory for storing the intermediate
# savepoint:
#   python3 flink/flink-job.py \
#       --env codfw \
#       --job-name "WDQS Streaming Updater" \
#       redeploy \
#       --jar lib/streaming-updater-producer-0.3.77-jar-with-dependencies.jar \
#       --options-file flink/rdf-streaming-updater.yaml \
#       --savepoint savepoints
#
# Schedule a savepoint for the job running in eqiad under savepoints:
#   python3 flink/flink-job.py \
#       --env eqiad \
#       --job-name "WDQS Streaming Updater" \
#       save \
#       --savepoint savepoints
#
# Stop with a savepoint the job running in eqiad under savepoints:
#   python3 flink/flink-job.py \
#       --env eqiad \
#       --job-name "WDQS Streaming Updater" \
#       stop \
#       --savepoint savepoints

import argparse
import json
import logging
import re
import socket
import sys
import zipfile
from datetime import datetime
from os import environ
from os.path import basename
from time import sleep
from typing import List, Optional
from urllib.parse import urljoin

import requests.packages.urllib3.util.connection as urllib3_cn
import yaml
from requests import Session

WDQS_JOB_NAME = 'WDQS Streaming Updater'
WCQS_JOB_NAME = 'WCQS Streaming Updater'

UPDATER_JOB_CLASS = 'org.wikidata.query.rdf.updater.UpdaterJob'

KAFKA_CLUSTER_FOR_STAGING = 'main-eqiad'

KAFKA_PORT = '9092'
# contains the config managed by puppet needed by the job
CONFIG_PATH = '/etc/helmfile-defaults/general-%s.yaml'

END_JOB_STATUS = ['FAILED', 'CANCELED', 'FINISHED']

ENDPOINT_PER_ENV = {
    'staging': 'https://staging.svc.eqiad.wmnet:4007',
    'eqiad': 'https://kubernetes1008.eqiad.wmnet:4007',  # there's no dedicated ns entry
    'codfw': 'https://kubernetes2007.codfw.wmnet:4007',  # there's no dedicated ns entry
}


def join_path(base, rest):
    sep = "" if base.endswith("/") or rest.startswith("/") else "/"
    return base + sep + rest


OBJECT_STORAGE_BASE = {
    'staging': {
        WDQS_JOB_NAME: join_path('s3://rdf-streaming-updater-staging', 'wikidata'),
        WCQS_JOB_NAME: join_path('s3://rdf-streaming-updater-staging', 'commons'),
    },
    'eqiad': {
        WDQS_JOB_NAME: join_path('s3://rdf-streaming-updater-eqiad', 'wikidata'),
        WCQS_JOB_NAME: join_path('s3://rdf-streaming-updater-eqiad', 'commons'),
    },
    'codfw': {
        WDQS_JOB_NAME: join_path('s3://rdf-streaming-updater-codfw', 'wikidata'),
        WCQS_JOB_NAME: join_path('s3://rdf-streaming-updater-codfw', 'commons'),
    }
}


OPTIONS_PER_ENV = {
    'staging': {
        'KAFKA_BROKERS': lambda c: c.extract_kafka_brokers(kafka_cluster='main-eqiad'),
        'HTTP_ROUTES': lambda c: c.build_http_routes(),
        'KAFKA_DC': 'eqiad',
        'PARALLELISM': 4,
        'OUTPUT_TOPIC_PARTITION': 0,
        'JOB_NAME': lambda c: c.job_name
    },
    'eqiad': {
        'KAFKA_BROKERS': lambda c: c.extract_kafka_brokers(kafka_cluster='main-eqiad'),
        'HTTP_ROUTES': lambda c: c.build_http_routes(),
        'KAFKA_DC': 'eqiad',
        'PARALLELISM': 12,
        'OUTPUT_TOPIC_PARTITION': 0,
        'JOB_NAME': lambda c: c.job_name
    },
    'codfw': {
        'KAFKA_BROKERS': lambda c: c.extract_kafka_brokers(kafka_cluster='main-codfw'),
        'HTTP_ROUTES': lambda c: c.build_http_routes(),
        'KAFKA_DC': 'codfw',
        'PARALLELISM': 12,
        'OUTPUT_TOPIC_PARTITION': 0,
        'JOB_NAME': lambda c: c.job_name
    }
}

WDQS_RECONCILE_PREFIX='wdqs_sideoutputs_reconcile'
WCQS_RECONCILE_PREFIX='wcqs_sideoutputs_reconcile'
RECONCILE_TOPIC='rdf-streaming-updater.reconcile'

# Per-job options that extend OPTIONS_PER_ENV, split by env
JOBS = {
    WDQS_JOB_NAME: {
        'common': {
            'MW_ACCEPTABLE_LAG': 10,
            'ENTITY_NAMESPACES': '0,120,146',
            'MEDIAINFO_ENTITY_NAMESPACES': None
        },
        'staging': {
            'KAFKA_CONSUMER_GROUP': 'wdqs_streaming_updater_test',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['staging'][WDQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'eqiad.rdf-streaming-updater.mutation-staging',
            'HOSTNAME': 'test.wikidata.org',
            'URIS_SCHEME': 'wikidata',
            'WIKIDATA_CONCEPT_URI': 'http://test.wikidata.org',
            'COMMONS_CONCEPT_URI': None,
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WDQS_RECONCILE_PREFIX,
                                                                 dc='eqiad')
        },
        'eqiad': {
            'KAFKA_CONSUMER_GROUP': 'wdqs_streaming_updater',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['eqiad'][WDQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'eqiad.rdf-streaming-updater.mutation',
            'HOSTNAME': 'www.wikidata.org',
            'URIS_SCHEME': 'wikidata',
            'WIKIDATA_CONCEPT_URI': None,
            'COMMONS_CONCEPT_URI': None,
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WDQS_RECONCILE_PREFIX,
                                                                 dc='eqiad')
        },
        'codfw': {
            'KAFKA_CONSUMER_GROUP': 'wdqs_streaming_updater',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['codfw'][WDQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'codfw.rdf-streaming-updater.mutation',
            'HOSTNAME': 'www.wikidata.org',
            'URIS_SCHEME': 'wikidata',
            'WIKIDATA_CONCEPT_URI': None,
            'COMMONS_CONCEPT_URI': None,
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WDQS_RECONCILE_PREFIX,
                                                                 dc='codfw')
        },
    },
    WCQS_JOB_NAME: {
        'common': {
            'MW_ACCEPTABLE_LAG': 15,
            'ENTITY_NAMESPACES': None,
            'MEDIAINFO_ENTITY_NAMESPACES': '6'
        },
        'staging': {
            'KAFKA_CONSUMER_GROUP': 'wcqs_streaming_updater_test',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['staging'][WCQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'eqiad.mediainfo-streaming-updater.mutation-staging',
            'HOSTNAME': 'test-commons.wikimedia.org',
            'URIS_SCHEME': 'commons',
            'WIKIDATA_CONCEPT_URI': 'http://test.wikidata.org',
            'COMMONS_CONCEPT_URI': 'https://test-commons.wikidata.org',
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WCQS_RECONCILE_PREFIX,
                                                                 dc='eqiad')
        },
        'eqiad': {
            'KAFKA_CONSUMER_GROUP': 'wcqs_streaming_updater',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['eqiad'][WCQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'eqiad.mediainfo-streaming-updater.mutation',
            'HOSTNAME': 'commons.wikimedia.org',
            'URIS_SCHEME': 'commons',
            'WIKIDATA_CONCEPT_URI': None,
            'COMMONS_CONCEPT_URI': None,
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WCQS_RECONCILE_PREFIX,
                                                                 dc='eqiad')
        },
        'codfw': {
            'KAFKA_CONSUMER_GROUP': 'wcqs_streaming_updater',
            'CHECKPOINT_DIR': join_path(OBJECT_STORAGE_BASE['codfw'][WCQS_JOB_NAME], "checkpoints"),
            'OUTPUT_TOPIC': 'codfw.mediainfo-streaming-updater.mutation',
            'HOSTNAME': 'commons.wikimedia.org',
            'URIS_SCHEME': 'commons',
            'WIKIDATA_CONCEPT_URI': None,
            'COMMONS_CONCEPT_URI': None,
            'RECONCILIATION_TOPIC': '{topic}[{tag}@{dc}]'.format(topic=RECONCILE_TOPIC,
                                                                 tag=WCQS_RECONCILE_PREFIX,
                                                                 dc='codfw')
        }
    },
}


def force_ipv4():
    """Force ipv4 because kubernetes hosts do not seem to support ipv6. This avoid waiting for a 30 sec timeout when
    establishing the connection."""

    def allowed_gai_family():
        family = socket.AF_INET
        return family

    urllib3_cn.allowed_gai_family = allowed_gai_family


class SessionWithBaseUrl(Session):
    def __init__(self, base_url=None):
        super(SessionWithBaseUrl, self).__init__()
        # skip SSL verif as we don't have actual ns entries
        # for rdf-streaming-updater.svc.[eqiad|codfw].wmnet
        self.verify = False
        self.base_url = base_url

    def request(self, method, url, *args, **kwargs):
        url = urljoin(self.base_url, url)
        return super(SessionWithBaseUrl, self).request(method, url, *args, **kwargs)


class Flink:
    def __init__(self, base_url):
        self.logger = logging.getLogger('Flink')
        try:
            base = environ['FLINK_ENDPOINT']
        except KeyError:
            base = base_url
        self.logger.debug("Using %s as flink endpoint", base)
        self.session = SessionWithBaseUrl(base_url=base)

    def get_jobs(self, active: bool = True) -> List[dict]:
        resp = self.session.get('jobs/overview')
        resp.raise_for_status()
        job_list = resp.json()
        return [{'jid': job['jid'], 'name': job['name'], 'details': job} for job in job_list['jobs'] if
                not active or job['state'] not in END_JOB_STATUS]

    def get_jobs_with_name(self, job_name: str) -> List[dict]:
        flink_jobs = self.get_jobs(active=False)
        return [job for job in flink_jobs if job['name'] == job_name]

    def get_jar_id_if_deployed(self, jar_file: str) -> Optional[str]:
        jar_name = basename(jar_file)
        resp = self.session.get('jars')
        resp.raise_for_status()
        jar_list = resp.json()['files']
        return next((j['id'] for j in jar_list if j['name'] == jar_name), None)

    def delete_jar(self, jar_id: str):
        self.logger.debug("Deleting jar_id %s", jar_id)
        resp = self.session.delete("jars/%s" % jar_id)
        resp.raise_for_status()

    def upload_jar(self, jar_file: str) -> str:
        jar_id = self.get_jar_id_if_deployed(jar_file)
        if jar_id is not None:
            self.logger.debug("Found matching jar_id %s for jar %s: re-uploading", jar_id, jar_file)
            self.delete_jar(jar_id)
        self.logger.debug("Jar missing for %s, uploading", jar_file)
        filename = basename(jar_file)
        resp = self.session.post("jars/upload",
                                 files={"jarfile": (filename, open(jar_file, 'rb'), "application/x-java-archive")})
        resp.raise_for_status()
        return basename(resp.json()['filename'])

    def schedule_job(self, jar_file: str,
                     entry_class: str,
                     job_options: dict,
                     save_point_path: Optional[str] = None,
                     allow_non_restored_state: bool = False) -> str:
        jar_id = self.upload_jar(jar_file)
        job_args = {
            'entryClass': entry_class,
            'allowNonRestoredState': allow_non_restored_state
        }
        if save_point_path is not None:
            job_args['savepointPath'] = save_point_path

        if len(job_options) > 0:
            # flatten arg list
            job_args['programArgsList'] = [k_or_v for k_and_v in job_options.items() for k_or_v in k_and_v]

        job_args_as_json = json.dumps(job_args)
        if allow_non_restored_state:
            self.logger.warning("Starting job with --allow-non-restored-state true")
        self.logger.debug("Starting job with arguments: %s", job_args_as_json)
        resp = self.session.post('jars/%s/run' % jar_id, data=job_args_as_json)
        resp.raise_for_status()
        jobid = resp.json()['jobid']
        self.logger.debug("Job submitted with id %s", jobid)
        while True:
            state = self.get_job_state(jobid)
            self.logger.debug("Job id %s with state %s", jobid, state)
            if state not in ['DEPLOYING', 'SCHEDULED']:
                break
        return jobid

    def get_job_state(self, job_id: str) -> str:
        resp = self.session.get('jobs/%s' % job_id)
        resp.raise_for_status()
        return resp.json()['state']

    def job_id_by_name(self, job_name: str) -> Optional[str]:
        jobs = self.get_jobs()
        return next((job['jid'] for job in jobs if job['name'] == job_name), None)

    def stop(self, job_name: str, savepoint_path: str, drain: bool = False) -> str:
        job_id = self.job_id_by_name(job_name)
        if job_id is None:
            raise ValueError("No job is running with name %s" % job_name)
        data = {
            'targetDirectory': savepoint_path,
            'drain': drain
        }
        resp = self.session.post("/jobs/%s/stop" % job_id, json=data)
        resp.raise_for_status()
        trigger_id = resp.json()['request-id']
        return self.wait_for_savepoint(job_id, trigger_id)

    def cancel(self, job_id: str):
        path = "/jobs/%s" % job_id
        self.logger.debug("XXX: Canceling job with PATH: %s", path)
        resp = self.session.patch(path)
        resp.raise_for_status()

    def trigger_savepoint(self, job_name: str, savepoint_path: str) -> str:
        job_id = self.job_id_by_name(job_name)
        if job_id is None:
            raise ValueError("No job is running with name %s" % job_name)
        data = {
            'target-directory': savepoint_path,
            'cancel-job': False
        }
        resp = self.session.post("/jobs/%s/savepoints" % job_id, json=data)
        resp.raise_for_status()
        trigger_id = resp.json()['request-id']
        return self.wait_for_savepoint(job_id, trigger_id)

    def wait_for_savepoint(self, job_id: str, trigger_id: str) -> str:
        self.logger.debug("Got request-id %s", trigger_id)
        while True:
            status = self.get_savepoint_progress(job_id, trigger_id)
            if status['status'] != 'IN_PROGRESS':
                break
            sleep(1)
        if status['status'] == 'ERROR':
            raise ValueError('Failed to take savepoint for job %s with cause %s' % (job_id, status['cause']))
        elif status['status'] == 'SUCCESS':
            return status['savepoint']
        raise ValueError('Unsupported status: %s', status)

    def get_savepoint_progress(self, job_id: str, trigger_id: str) -> dict:
        resp = self.session.get("/jobs/%s/savepoints/%s" % (job_id, trigger_id))
        resp.raise_for_status()
        json_resp = resp.json()
        self.logger.debug("Savepoint status for: %s", json_resp)
        ret = {'status': json_resp['status']['id']}
        op = json_resp['operation']
        if ret['status'] == 'COMPLETED' and 'location' in op.keys():
            return {
                'status': 'SUCCESS',
                'savepoint': op['location']
            }
        elif ret['status'] == 'COMPLETED' and 'failure-cause' in op.keys():
            return {
                'status': 'ERROR',
                'cause': op['failure-cause']['stack-trace']
            }
        elif ret['status'] == 'IN_PROGRESS':
            return {
                'status': 'IN_PROGRESS',
            }
        else:
            raise ValueError('Unsupported response %s', json_resp)


class JobConf:
    def __init__(self, env: str, job_name: str):
        file = CONFIG_PATH % env
        self.config = yaml.safe_load(open(file))
        if self.config is None:
            raise IOError("Cannot read %s" % file)
        self.job_option_replacement = dict(OPTIONS_PER_ENV[env])
        self.job_option_replacement.update(JOBS[job_name]['common'])
        self.job_option_replacement.update(JOBS[job_name][env])
        self.job_name = job_name
        self.object_store_base = OBJECT_STORAGE_BASE[env][job_name]
        self.logger = logging.getLogger("jobconf")

    def get_service_proxy_url(self, service: str) -> str:
        proxy_config = self.config['services_proxy'][service]
        return "http://localhost:%s" % str(proxy_config['port'])

    def extract_kafka_brokers(self, kafka_cluster):
        ips = list(
            map(lambda s: s[0:-3] + ':' + KAFKA_PORT,
                filter(lambda s: s.endswith('/32'), self.config['kafka_brokers'][kafka_cluster])
                )
        )
        return ','.join(ips)

    def build_http_routes(self) -> str:
        schema_endpoint = self.get_service_proxy_url('schema')
        mediawiki_endpoint = self.get_service_proxy_url('mwapi-async')
        return ','.join([
            'www.wikidata.org=' + mediawiki_endpoint,
            'test.wikidata.org=' + mediawiki_endpoint,
            'meta.wikimedia.org=' + mediawiki_endpoint,
            'commons.wikimedia.org=' + mediawiki_endpoint,
            'test-commons.wikimedia.org=' + mediawiki_endpoint,
            'schema.wikimedia.org=' + schema_endpoint,
        ])

    def get_job_options(self, option_yaml_file) -> dict:
        common_options = yaml.safe_load(open(option_yaml_file, 'r'))

        def replace(v):
            if isinstance(v, bool):
                return str(v).lower()

            if not isinstance(v, str):
                return v

            m = re.search(r'\$(.+)\$', v)
            var_val = v
            if m:
                var_name = m.group(1)
                try:
                    replacement = self.job_option_replacement[var_name]
                except KeyError:
                    raise ValueError("Missing replacement for %s" % var_name)
                if callable(replacement):
                    replacement = replacement(self)
                if replacement is None:
                    # prefers application defaults
                    return None
                var_val = var_val[:m.start()] + str(replacement) + var_val[m.end():]

            return var_val

        # build a map --option_name: option_value
        options = {('--' + k): replace(v) for k, v in common_options.items() if replace(v) is not None}
        return options

    def build_object_store_path(self, savepoint: str) -> str:
        if re.match(r"[a-z]+://", savepoint) is not None:
            # return unmodified if it's a full URL
            if not savepoint.startswith(self.object_store_base):
                self.logger.warning("Using non standard object store path %s expected a path starting with %s",
                                    savepoint, self.object_store_base)
            return savepoint
        sep = "" if self.object_store_base.endswith("/") or savepoint.startswith("/") else "/"
        return self.object_store_base + sep + savepoint


class InsecureRequestWarningFilter(logging.Filter):
    """Hack to avoid printing the annoying warning from /urllib3/connectionpool.py about insecure connections.
    We don't have much choices here since k8s hosts do not have dedicated certs.
    """
    @staticmethod
    def capture_insecure_request_warnings():
        logging.getLogger("py.warnings").addFilter(InsecureRequestWarningFilter())
        logging.captureWarnings(True)

    def filter(self, record):
        return "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification " \
                   "is strongly" not in record.getMessage()


def check_jar(jar_file: str) -> str:
    """Make sure the jar file is valid before sending any commands to the flink cluster"""
    with zipfile.ZipFile(jar_file, "r") as zip:
        if zip.testzip() is not None:
            raise ValueError(f"{jar_file} is not valid")
    return jar_file


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)5.5s %(asctime)s [%(name)7.7s] %(message)s',
                        datefmt="%Y-%m-%dT%H:%M:%S%z")
    InsecureRequestWarningFilter.capture_insecure_request_warnings()
    logger = logging.getLogger()

    force_ipv4()

    parser = argparse.ArgumentParser(description='Flink job deployer')
    parser.add_argument("--debug", required=False, action="store_true")
    parser.add_argument("--job-name", required=True, choices=JOBS.keys())
    parser.add_argument("--env", required=True, choices=OPTIONS_PER_ENV.keys())
    subparsers = parser.add_subparsers(dest='action')

    deploy_parser = subparsers.add_parser('deploy')
    deploy_parser.add_argument("--jar", required=True, type=check_jar)
    deploy_parser.add_argument("--flink-job-class", required=False, default=UPDATER_JOB_CLASS)
    deploy_parser.add_argument("--options-file", required=True, help='Path to the default options (yaml format)')
    deploy_parser.add_argument("--initial-state", required=False, help='Initial state')
    deploy_parser.add_argument("--ignore-failures-after-transaction-timeout", required=False,
                               help='DANGEROUS, workaround to resume from an old savepoint',
                               action="store_true")
    deploy_parser.add_argument("--allow-non-restored-state", required=False,
                                 help='DANGEROUS, useful only when structural changes are made to the app',
                                 action='store_true')

    redeploy_parser = subparsers.add_parser('redeploy')
    redeploy_parser.add_argument("--jar", required=True, type=check_jar)
    redeploy_parser.add_argument("--flink-job-class", required=False, default=UPDATER_JOB_CLASS)
    redeploy_parser.add_argument("--options-file", required=True, help='Path to the default options (yaml format)')
    redeploy_parser.add_argument("--savepoint", required=True)
    redeploy_parser.add_argument("--allow-non-restored-state", required=False,
                                 help='DANGEROUS, useful only when structural changes are made to the app',
                                 action='store_true')

    save_parser = subparsers.add_parser('save')
    save_parser.add_argument("--savepoint", required=True)

    stop_parser = subparsers.add_parser('stop')
    stop_parser.add_argument("--savepoint", required=True)

    # risky procedure but required as a workaround for https://issues.apache.org/jira/browse/FLINK-28758
    save_and_cancel = subparsers.add_parser('save_and_cancel')
    save_and_cancel.add_argument("--savepoint", required=True)
    save_and_cancel.add_argument("--job-id", required=True)

    subparsers.add_parser('status')

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    flink = Flink(ENDPOINT_PER_ENV[args.env])
    jname = args.job_name
    conf = JobConf(env=args.env, job_name=jname)

    if args.action == 'deploy':
        logger.debug("Deploying %s", jname)
        jobs = flink.get_jobs()
        existing_job = next((job for job in jobs if job['name'] == jname), None)
        if existing_job is not None:
            sys.exit("ERROR: Job with name %s already running with job id: %s" % (args.job_name, existing_job['jid']))

        options = conf.get_job_options(args.options_file)
        if args.ignore_failures_after_transaction_timeout:
            options["--ignore_failures_after_transaction_timeout"] = "true"
        initial_state = None
        if args.initial_state is not None:
            initial_state = conf.build_object_store_path(args.initial_state)
        jid = flink.schedule_job(jar_file=args.jar,
                                 entry_class=args.flink_job_class,
                                 job_options=options,
                                 save_point_path=initial_state,
                                 allow_non_restored_state=args.allow_non_restored_state)
        logger.info("Job %s deployed with id %s" % (jname, jid))
    elif args.action == 'save':
        logger.debug("Saving job %s", jname)
        savepoint_base = conf.build_object_store_path(savepoint=args.savepoint)
        savepoint_path = flink.trigger_savepoint(job_name=jname, savepoint_path=savepoint_base)
        logger.info("Job %s saved at %s", jname, savepoint_path)
    elif args.action == 'stop':
        logger.debug("Stopping job %s", jname)
        savepoint_base = conf.build_object_store_path(savepoint=args.savepoint)
        savepoint_path = flink.stop(job_name=jname, savepoint_path=savepoint_base)
        logger.info("Job %s saved at %s", jname, savepoint_path)
    elif args.action == 'status':
        logger.debug("Status job %s", jname)
        jobs = flink.get_jobs_with_name(job_name=jname)
        for job in jobs:
            start_time = datetime.fromtimestamp(job['details']['start-time'] / 1000).isoformat()
            end_time = 'N/A'
            if job['details']['end-time'] > 0:
                end_time = datetime.fromtimestamp(job['details']['end-time'] / 1000).isoformat()
            logger.info("Job name:%s, id:%s, state:%s, started:%s, ended:%s",
                        jname, job['jid'], job['details']['state'], start_time, end_time)
    if args.action == 'redeploy':
        logger.debug("Redeploying job %s", jname)
        options = conf.get_job_options(args.options_file)
        savepoint_base = conf.build_object_store_path(savepoint=args.savepoint)
        savepoint_path = flink.stop(job_name=jname, savepoint_path=savepoint_base)
        logger.info("Job %s saved at %s", jname, savepoint_path)
        jid = flink.schedule_job(jar_file=args.jar,
                                 entry_class=args.flink_job_class,
                                 job_options=options,
                                 save_point_path=savepoint_path,
                                 allow_non_restored_state=args.allow_non_restored_state)
        logger.info("Job %s redeployed with id %s" % (jname, jid))
    elif args.action == 'save_and_cancel':
        logger.debug("Saving and cancelling job %s", jname)
        job_id = flink.job_id_by_name(jname)
        if job_id is None:
            raise "Job %s is not running" % jname
        if job_id != args.job_id:
            raise "Job %s is not running with id %s" % (jname, args.job_id)
        logger.debug("Saving job %s", jname)
        savepoint_base = conf.build_object_store_path(savepoint=args.savepoint)
        savepoint_path = flink.trigger_savepoint(job_name=jname, savepoint_path=savepoint_base)
        logger.info("Job %s saved at %s", jname, savepoint_path)
        flink.cancel(job_id)


if __name__ == "__main__":
    main()
