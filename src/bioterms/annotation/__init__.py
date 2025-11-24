import importlib
import importlib.resources

from bioterms.etc.enums import ConceptPrefix


def get_annotation_module(prefix_1: ConceptPrefix,
                          prefix_2: ConceptPrefix,
                          ):
    """
    Get the annotation module for the given pair of prefixes.
    :param prefix_1: The first prefix.
    :param prefix_2: The second prefix.
    :return: The annotation module.
    """
    prefix_str_1 = prefix_1.value.lower()
    prefix_str_2 = prefix_2.value.lower()

    sorted_prefixes = sorted([prefix_str_1, prefix_str_2])
    annotation_module_name = f'{sorted_prefixes[0]}_{sorted_prefixes[1]}'

    annotation_module = importlib.import_module(f'bioterms.annotation.{annotation_module_name}')

    return annotation_module
