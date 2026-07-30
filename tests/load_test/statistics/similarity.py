import argparse
from load_test.statistics.benchmark import add_common_arguments, run_suite
from load_test.statistics.endpoints import SIMILARITY

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Similarity endpoint benchmark")
    add_common_arguments(parser)
    run_suite(SIMILARITY, parser.parse_args())
