# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import requests
from jx_python import jx
from mo_json import value2json, json2value
from mo_logs import Log, startup
from mo_times import Date
from pyLibrary import aws

DEBUG = False
ACTIVEDATA = "https://activedata.allizom.org/query"


def status(config):
    """
    PRINT OUT THE CODE COVERAGE ETL PIPELINE STATUS
    """
    sent_for_rerun = set()
    work_queue = aws.Queue(config.work_queue)

    # determine the tasks that generated coverage
    response = requests.get(ACTIVEDATA, data=value2json({
        "from": "task",
        "select": [
            "_id",
            {"name": "date", "value": "repo.changeset.date"},
            {"name": "task", "value": "task.id"},
            {"name": "rev", "value": "repo.changeset.id12"},
            {"name": "branch", "value": "repo.branch.name"}
        ],
        "where": {"and": [
            # {"in": {"repo.changeset.id12": ["dcb3a3ba9065", "6369d1c6526b"]}},
            {"eq": {"treeherder.jobKind": "test"}},
            # {"eq": {"repo.branch.name": "mozilla-central"}},
            {"eq": {"task.run.state": "completed"}},
            {"or": [
                {"eq": {"build.type": "ccov"}}
            ]},
            {"lt": {"action.end_time": {"date": "now-6hour"}}},
            {"gte": {"action.end_time": {"date": "now-3day"}}}
        ]},
        "limit": 10000,
        "format": "list"
    }))
    coverage_runs = json2value(response.content.decode("utf8")).data

    # FOR EACH REVISION, GET STATS
    for g, runs in jx.groupby(coverage_runs, ["rev", "branch"]):
        # find tasks in `coverage` table
        total_tasks = set(runs.task)
        if DEBUG:
            Log.note("{{num}} tasks:\n{{tasks}}", num=len(total_tasks), tasks=jx.sort(total_tasks))
        response = requests.get(ACTIVEDATA, data=value2json({
            "from": "coverage",
            "edges": {"name": "task", "value": "task.id"},
            "where": {"eq": {"repo.changeset.id12": g.rev}},
            "format": "list",
            "limit": 10000
        }))

        ingested_tasks = set(json2value(response.content.decode("utf8")).data.task) - {None}
        if DEBUG:
            Log.note("{{num}} ingested:\n{{tasks}}", num=len(ingested_tasks), tasks=jx.sort(ingested_tasks))

        task_rate = len(ingested_tasks) / len(total_tasks)

        files_processed = {}
        summary_files = {}
        file_rate = 0.0

        # find files in the `coverage` table
        if ingested_tasks:
            response = requests.get(ACTIVEDATA, data=value2json({
                "from": "coverage",
                "edges": {"name": "file", "value": "source.file.name"},
                "where": {"eq": {"repo.changeset.id12": g.rev}},
                "format": "list",
                "limit": 100000
            }))
            files_processed = set(json2value(response.content.decode("utf8")).data.file) - {None}

            # find files in the `coverage` table
            response = requests.get(ACTIVEDATA, data=value2json({
                "from": "coverage-summary",
                "edges": {"name": "file", "value": "source.file.name"},
                "where": {"eq": {"repo.changeset.id12": g.rev}},
                "format": "list",
                "limit": 100000
            }))
            summary_files = set(json2value(response.content.decode("utf8")).data.file) - {None}
            if files_processed:
                file_rate = len(summary_files) / len(files_processed)
            else:
                file_rate = 0.0

        Log.note(
            "{{branch}}-{{rev}} - {{rate|percent}} DONE - {{date|datetime}} Tasks {{ingested_tasks}}/{{total_tasks}} ({{task_rate|percent}})  Files {{summary_files}}/{{files_processed}} ({{file_rate|percent}})",
            branch=g.branch,
            rev=g.rev,
            date=runs[0].date,
            rate=task_rate * file_rate,
            ingested_tasks=len(ingested_tasks),
            total_tasks=len(total_tasks),
            task_rate=task_rate,
            summary_files=len(summary_files),
            files_processed=len(files_processed),
            file_rate=file_rate
        )

        missing_tasks = total_tasks - ingested_tasks
        if missing_tasks:
            Log.note("{{num}} MISSING TASKS : {{missing|json}}", missing=sorted(list(missing_tasks))[:3], num=len(missing_tasks))

            missing_runs = set(
                run._id.split(":")[0]
                for miss in missing_tasks
                for run in coverage_runs
                if run.task == miss
            )

            net_send = missing_runs - sent_for_rerun
            sent_for_rerun |= missing_runs

            Log.note("sending keys for rerun (not already sent): {{keys|json}}", keys=net_send)
            work_queue.extend([
                {
                    "key": id,
                    "bucket": "active-data-task-cluster-normalized",
                    "destination": "active-data-codecoverage",
                    "timestamp": Date.now()
                }
                for id in net_send
            ])
        else:
            # BE MORE DISCRIMINATING
            missing_files = files_processed - summary_files
            if missing_files:
                Log.note("{{num}} MISSING FILES : {{missing|json}}", missing=sorted(list(missing_files))[:3], num=len(missing_files))


if __name__ == "__main__":
    try:
        config = startup.read_settings()
        Log.start(config.debug)
        status(config)
    except Exception as e:
        Log.error("Problem with code coverage score calculation", cause=e)
    finally:
        Log.stop()


