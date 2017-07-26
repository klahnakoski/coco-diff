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
from mo_dots import FlatList, listwrap, wrap, set_default
from mo_json import json2value, value2json
from mo_logs import Log, startup, constants
from mo_times import Date
from pyLibrary import aws
from pyLibrary.queries import jx
from pyLibrary.queries.expression_compiler import compile_expression
from pyLibrary.queries.expressions import jx_expression

ACTIVE_DATA_URL = "http://activedata.allizom.org/query"
SHOW_MISSING = False


def diff(a_name, a_filter, b_name, b_filter):
    # COLLECT ALL COVERAGE FROM THE TWO VARIATIONS
    variables = jx_expression(a_filter).vars() | jx_expression(a_filter).vars()

    # HOW MANY FILES ARE THERE?
    result = requests.post(
        ACTIVE_DATA_URL,
        json={
            "from": "coverage",
            "select": [
                {"aggregate": "count"},
            ],
            "groupby": "source.file.name",
            "where": {"and": [
                {"or": [a_filter, b_filter]},
                {"eq": {"source.is_file": "T"}},
                {"gt": {"source.file.total_covered": 0}}
            ]},
            "limit": 50000,
            "format": "table"
        }
    )
    source_files = json2value(result.content.decode('utf8')).data
    Log.note("{{num}} unique files covered", num=len(source_files))

    def groupby():
        count = 0
        output = []
        for f, c in source_files:
            count += c
            output.append(f)
            if count >= 5000:
                yield count, output
                count = 0
                output = []
        if output:
            yield count, output

    a_coverage = {}  # MAP FROM FILENAME TO SET OF LINES COVERED
    b_coverage = {}

    is_a = compile_expression(jx_expression(a_filter).to_python())
    is_b = compile_expression(jx_expression(b_filter).to_python())

    for g, files in groupby():
        Log.note("get {{source}} source files", source=len(files))
        raw_result = requests.post(
            ACTIVE_DATA_URL,
            data=value2json({
                "from": "coverage",
                "select": {"source.file.covered", "source.file.name"} | variables,
                "where": {"and": [
                    {"or": [a_filter, b_filter]},
                    {"terms": {"source.file.name": files}}
                ]},
                "limit": 50000,
                "format": "list"
            }).encode('utf8')
        )
        data = json2value(raw_result.content.decode('utf8')).data
        Log.note("got {{source}} source files ({{records}} records)", source=len(files), records=len(data))
        for d in data:
            filename = d.source.file.name
            lines = listwrap(d.source.file.covered.line)

            if is_a(d, 0, [d]):
                cover = a_coverage.get(filename)
                if not cover:
                    cover = a_coverage[filename] = set()
                cover.update(lines)

            if is_b(d, 0, [d]):
                cover = b_coverage.get(filename)
                if not cover:
                    cover = b_coverage[filename] = set()
                cover.update(lines)

    # SUBTRACT COVERAGE
    a_has_extra = FlatList()
    for filename, a_cover in a_coverage.items():
        b_cover = b_coverage.get(filename, set())
        remainder = a_cover - b_cover
        if remainder:
            a_has_extra.append({
                "file": filename,
                "count": len(remainder),
                "a_name": a_name,
                "a": len(a_cover),
                "b_name": b_name,
                "b": len(b_cover),
                "remainder": len(remainder)
            })

    b_has_extra = FlatList()
    for filename, b_cover in b_coverage.items():
        a_cover = a_coverage.get(filename, set())
        remainder = b_cover - a_cover
        if remainder:
            b_has_extra.append({
                "file": filename,
                "count": len(remainder),
                "a_name": a_name,
                "a": len(a_cover),
                "b_name": b_name,
                "b": len(b_cover),
                "remainder": len(remainder)
            })

    # SHOW LARGEST DIFF FIRST
    for d in jx.sort(a_has_extra, {"count": "desc"})[0:20:]:
        Log.note("{{a_name}} ({{a}} lines) has additional {{remainder}} lines over {{b_name}} ({{b}} lines) in {{file}}", d)
    Log.note("---")
    for d in jx.sort(b_has_extra, {"count": "desc"})[0:20:]:
        Log.note("{{b_name}} ({{b}} lines) has additional {{remainder}} lines over {{a_name}} ({{a}} lines) in {{file}}", d)


def confirm_coverage(
    settings,
    _filter,
    groupby="repo.changeset.id12",
    add_missing_to_queue=False
):
    """
    CONFIRM WE HAVE COVERAGE 
    """
    Log.note("begin review")

    # ALL TASKS FOR A REVISION
    result = requests.post(
        ACTIVE_DATA_URL,
        json={
            "from": "task",
            "select": (
                [
                    {"name": "id", "value": "etl.id"},
                    {"name": "source.id", "value": "etl.source.id"},
                ] + list(
                    {
                        "repo.changeset.id12",
                        "build.type",
                        "run.type",
                        "run.suite",
                        "action.start_time"
                    } |
                    set(listwrap(groupby))
                )
            ),
            "where": {"and": [
                _filter,
                {"eq": {"build.type": "ccov"}}
            ]},
            "format": "list",
            "limit": 10000
        }
    )
    all_tasks = json2value(result.content.decode('utf8')).data

    for g, tasks in jx.groupby(all_tasks, groupby):
        # FIND ALL COVERAGE
        result = requests.post(
            ACTIVE_DATA_URL,
            json={
                "from": "coverage",
                "groupby": [
                    {"name": "source.id", "value": "etl.source.source.source.id"},
                    {"name": "id", "value": "etl.source.source.id"}
                ],
                "where": {"and": [
                    _filter,
                    {"or": [
                        {"eq": {
                            "etl.source.source.source.id": e.source.id,
                            "etl.source.source.id": e.id
                        }}
                        for e in tasks
                    ]}
                ]},
                "format": "list",
                "limit": 10000
            }
        )
        coverage = json2value(result.content.decode('utf8')).data
        Log.note("found {{num}} coverage", num=len(coverage))
        # Log.note("found {{coverage}}", coverage=coverage)
        # REVIEW
        found = []
        not_found = []
        for task in tasks:
            for cov in coverage:
                if cov.count < 1000:
                    continue
                if cov.id == task.id and cov.source.id == task.source.id:
                    found.append(task)
                    break
            else:
                not_found.append(task)

        summary = set_default({"tasks": len(tasks), "coverage": len(tasks)-len(not_found)}, g)
        Log.note("Summary\n{{ccov|json}}", ccov=summary)
        if SHOW_MISSING:
            Log.note("Details\n{{missing}}", missing=not_found)

        # TRIGGER REPROCESSING
        if add_missing_to_queue:
            work_queue = aws.Queue(kwargs=settings.work_queue)
            work_queue.extend([
                {
                    "key": "tc." + unicode(n.source.key),
                    "bucket": "active-data-taskcluster-normalized",
                    "destination": "active-data-codecoverage",
                    "timestamp": Date.now()
                }
                for n in not_found
            ])


def verify_past_coverage(settings):
    confirm_coverage(
        settings,
        {"and": [
            {"eq": {"repo.changeset.id12": "c55e582aee5f"}},
            # {"gte": {"action.start_time": {"date": "today"}}},
            # {"lt": {"action.start_time": {"date": "today+day"}}}
        ]},
        groupby=["repo.changeset.id12", "repo.push.date"]
    )





def main():
    # FIND A REVISION WITH e10s
    # {
    # 	"from":"task",
    # 	"select":[{"value":"run.timestamp","aggregate":"max"}],
    # 	"groupby":["repo.changeset.id12"],
    # 	"where":{"and":[
    # 		{"regex":{"run.name":".*cov.*"}},
    # 		{"eq":{"run.type":"e10s"}},
    # 		{"gt":{"run.timestamp":{"date":"today-4day"}}}
    # 	]}
    # }

    # SOME SAMPLE RECORDS
    # {
    # 	"from":"coverage",
    # 	"select":[
    # 		"source.file.name",
    # 		"source.file.total_covered",
    # 		"source.file.total_uncovered",
    # 		"build.type",
    # 		"run.type",
    # 		"run.suite.fullname",
    # 		"run.chunk"
    # 	],
    # 	"where":{"and":[
    # 		{"eq":{"repo.changeset.id12":"37d777d87200"}},
    # 		{"eq":{"run.suite.fullname":"mochitest-plain"}},
    # 		{"regex":{"source.file.name":".*sqlite.*"}}
    # 	]},
    # 	"limit":100
    # }

    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        verify_past_coverage(settings)

        # confirm_coverage(
        #     settings,
        #     {"and": [
        #         {"gte":{"action.start_date":{"date":"today-3day"}}},
        #         {"eq": {"run.suite.fullname": "firefox-ui-functional local"}},
        #         {"eq": {"repo.changeset.id12": "bc9e028dbdc5"}}
        #     ]}
        # )



        # a_filter = {"and": [
        #     {"eq": {"run.suite.fullname": "firefox-ui-functional local"}},
        #     {"eq": {"repo.changeset.id12": "bc9e028dbdc5"}},
        #     {"not": {"eq": {"run.type": "e10s"}}}
        # ]}
        # b_filter = {"and": [
        #     {"eq": {"run.suite.fullname": "firefox-ui-functional local"}},
        #     {"eq": {"repo.changeset.id12": "bc9e028dbdc5"}},
        #     {"eq": {"run.type": "e10s"}}
        # ]}
        #
        # diff("non-e10s", a_filter, "e10s", b_filter)



        # a_filter = {"and": [
        #     # {"regex": {"source.file.name": ".*sqlite.*"}},
        #     {"eq": {"repo.changeset.id12": "7c4ca88d519f"}},
        #     {"eq": {"run.suite.fullname": "mochitest-plain"}},
        #     {"not": {"eq": {"run.type": "e10s"}}}
        # ]}
        # b_filter = {"and": [
        #     # {"regex": {"source.file.name": ".*sqlite.*"}},
        #     {"eq": {"repo.changeset.id12": "7c4ca88d519f"}},
        #     {"eq": {"run.suite.fullname": "mochitest-plain"}},
        #     {"eq": {"run.type": "e10s"}}
        # ]}
        #
        # diff("non-e10s", a_filter, "e10s", b_filter)
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
