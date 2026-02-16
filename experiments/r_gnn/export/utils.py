from neo4j import Driver


def get_descendants(prefix: str,
                    parent_id: str,
                    driver: Driver
                    ) -> list[str]:
    with driver.session() as session:
        result = session.run(
            f"""
            MATCH (parent:Concept {{id: '{parent_id}', prefix: '{prefix}'}})<-[:IS_A*0..]-(descendant:Concept)
            RETURN descendant.id AS id
            """
        )

        return [str(record['id']) for record in result]
