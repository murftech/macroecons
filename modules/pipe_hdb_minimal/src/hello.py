"""Smallest possible task body, so the minimal bundle has something real to point at.

Note what is NOT here: no bundle variables, no ${...} anything. By the time this runs,
every variable has already been substituted into plain argv strings by the CLI on the
laptop. Databricks itself never saw the YAML.

Note also there is no __file__ available under spark_python_task - it runs through an
internal exec() wrapper. If this ever needs to import a sibling module, it has to be told
where it lives via a --src-dir parameter, the same workaround ../../src/01_ingest_bronze.py
uses.
"""

import sys


def main():
    print('hello from a minimal databricks job')
    print('argv:', sys.argv)


if __name__ == '__main__':
    main()
