"""Adaptive raw-statistics benchmark for all auto-complete versions."""

import argparse

from load_test.statistics.benchmark import add_common_arguments, run_suite
from load_test.statistics.endpoints import AUTO_COMPLETE


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    run_suite(AUTO_COMPLETE, parser.parse_args())


if __name__ == "__main__":
    main()
