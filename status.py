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
from mo_json import value2json, json2value
from mo_logs import Log
from pyLibrary.queries import jx

ACTIVEDATA = "http://activedata.allizom.org/query"


def status():
    """
    PRINT OUT THE CODE COVERAGE ETL PIPELINE STATUS
    """

    # determine the tasks that generated coverage
    response = requests.get(ACTIVEDATA, data=value2json({
        "from": "task",
        "select": [
            {"name": "date", "value": "repo.changeset.date"},
            {"name": "task", "value": "task.id"},
            {"name": "rev", "value": "repo.changeset.id12"}
        ],
        "where": {"and": [
            # {"eq": {"repo.changeset.id12": "60a5308fa987"}},
            {"eq": {"treeherder.jobKind": "test"}},
            {"eq": {"build.type": "ccov"}},
            {"gt": {"action.start_time": {"date": "today-3day"}}}
        ]},
        "limit": 10000,
        "format": "list"
    }))
    coverage_runs = jx.sort(json2value(response.content.decode("utf8")).data, {"date": "desc"})

    # FOR EACH REVISION, GET STATS
    for g, runs in jx.groupby(coverage_runs, "rev", contiguous=True):
        # find tasks in `coverage` table
        total_tasks = set(runs.task)
        response = requests.get(ACTIVEDATA, data=value2json({
            "from": "coverage",
            "edges": {"name": "task", "value": "task.id"},
            "where": {"eq": {"repo.changeset.id12": g.rev}},
            "format": "list",
            "limit": 10000
        }))

        ingested_tasks = set(json2value(response.content.decode("utf8")).data.task)
        task_rate = len(ingested_tasks) / len(total_tasks)

        files_processed = {}
        summary_files = {}
        file_rate = 0.0

        # find files in the `coverage` table
        if ingested_tasks:
            response = requests.get(ACTIVEDATA, data=value2json({
                "from": "coverage",
                "edges": {"name":"file", "value":"source.file.name"},
                "where": {"eq": {"repo.changeset.id12": g.rev}},
                "format": "list",
                "limit": 100000
            }))
            files_processed = set(json2value(response.content.decode("utf8")).data.file)

            # find files in the `coverage` table
            response = requests.get(ACTIVEDATA, data=value2json({
                "from": "coverage-summary",
                "edges": {"name":"file", "value":"source.file.name"},
                "where": {"eq": {"repo.changeset.id12": g.rev}},
                "format": "list",
                "limit": 100000
            }))
            summary_files = set(json2value(response.content.decode("utf8")).data.file)
            if files_processed:
                file_rate = len(summary_files) / len(files_processed)
            else:
                file_rate = 0.0

        Log.note(
            "{{rev}} - {{rate|percent}} DONE - {{date|datetime}} Tasks {{ingested_tasks}}/{{total_tasks}} ({{task_rate|percent}})  Files {{summary_files}}/{{files_processed}} ({{file_rate|percent}})",
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
        else:
            # BE MORE DISCRIMINATING
            missing_files = files_processed - summary_files
            if missing_files:
                Log.note("{{num}} MISSING FILES : {{missing|json}}", missing=sorted(list(missing_files))[:3], num=len(missing_files))


status()
