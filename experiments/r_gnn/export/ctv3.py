import json
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase

from utils import get_descendants


CONFIG_PATH = Path('../config.json')


def export_ctv3():
    config = json.loads(CONFIG_PATH.read_text())

    driver = GraphDatabase.driver(
        uri=config['neo4j']['uri'],
        auth=(config['neo4j']['user'], config['neo4j']['password']),
    )

    remaining_ids = set()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Concept {prefix: 'ctv3'})-[]-()
            RETURN c.id AS id
            """
        )

        for record in result:
            remaining_ids.add(str(record['id']))

    device_nodes = get_descendants(
        prefix='ctv3',
        parent_id='r....',  # APPLIANCES & REAGENTS ETC(2)
        driver=driver,
    )
    device_nodes += get_descendants(
        prefix='ctv3',
        parent_id='x00xl',  # Appliances+equipment
        driver=driver,
    )
    device_nodes = list(set(device_nodes))
    df = pd.DataFrame({
        'cid': device_nodes,
        'type': 'device',
    })
    remaining_ids -= set(device_nodes)

    organism_nodes = get_descendants(
        prefix='ctv3',
        parent_id='XM0Nm',  # Organisms
        driver=driver,
    )
    organism_nodes += get_descendants(
        prefix='ctv3',
        parent_id='XaBEy',  # XaBEy
        driver=driver,
    )
    organism_nodes = list(set(organism_nodes))
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': organism_nodes,
            'type': 'organism',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(organism_nodes)

    anatomy_nodes = get_descendants(
        prefix='ctv3',
        parent_id='X79tP',  # Anatomical concepts
        driver=driver,
    )
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': anatomy_nodes,
            'type': 'anatomy',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(anatomy_nodes)

    phenotype_nodes = get_descendants(
        prefix='ctv3',
        parent_id='XaBVJ',  # Clinical findings
        driver=driver,
    )
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': phenotype_nodes,
            'type': 'phenotype',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(phenotype_nodes)

    disorder_nodes = get_descendants(
        prefix='ctv3',
        parent_id='XaBvL',  # Extinct cross-type disorder and observation
        driver=driver,
    )
    disorder_nodes += get_descendants(
        prefix='ctv3',
        parent_id='XaBVn',  # Extinct cross-type disorder and procedure
        driver=driver,
    )
    disorder_nodes += get_descendants(
        prefix='ctv3',
        parent_id='X0003',  # Disorders
        driver=driver,
    )
    disorder_nodes = list(set(disorder_nodes))
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': disorder_nodes,
            'type': 'disorder',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(disorder_nodes)

    procedure_nodes = get_descendants(
        prefix='ctv3',
        parent_id='XaBEx',  # Extinct cross-type procedure
        driver=driver,
    )
    procedure_nodes += get_descendants(
        prefix='ctv3',
        parent_id='XaBVF', # Extinct cross-type investigation
        driver=driver,
    )
    procedure_nodes += get_descendants(
        prefix='ctv3',
        parent_id='Xa22Y',  # Operations, procedures and interventions
        driver=driver,
    )
    procedure_nodes = list(set(procedure_nodes))
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': procedure_nodes,
            'type': 'procedure',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(procedure_nodes)

    administration_nodes = get_descendants(
        prefix='ctv3',
        parent_id='XaBVL',  # Extinct cross-type administration
        driver=driver,
    )
    administration_nodes += get_descendants(
        prefix='ctv3',
        parent_id='9....',  # Administration
        driver=driver,
    )
    administration_nodes = list(set(administration_nodes))
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': administration_nodes,
            'type': 'administration',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(administration_nodes)

    substance_nodes = get_descendants(
        prefix='ctv3',
        parent_id='x00xm',  # Drug
        driver=driver,
    )
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': substance_nodes,
            'type': 'substance',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(substance_nodes)

    event_nodes = get_descendants(
        prefix='ctv3',
        parent_id='T....',  # Causes of injury and poisoning
        driver=driver,
    )
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': event_nodes,
            'type': 'event',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')
    remaining_ids -= set(event_nodes)

    # All remaining IDs are structural nodes
    df = pd.concat([
        df,
        pd.DataFrame({
            'cid': list(remaining_ids),
            'type': 'structural',
        }),
    ], ignore_index=True).drop_duplicates(subset='cid', keep='last')

    out_path = Path('../data/ctv3_node.parquet')

    df.to_parquet(out_path, index=False)
    print(f'Exported {df.shape[0]} nodes to {out_path}')


if __name__ == '__main__':
    export_ctv3()
