import argparse
from load_test.statistics.benchmark import add_common_arguments, run_suite
from load_test.statistics.endpoints import MAPPING

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mapping endpoint benchmark")
    add_common_arguments(parser)
    run_suite(MAPPING, parser.parse_args())
