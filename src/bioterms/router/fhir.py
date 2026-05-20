from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Request
from fhir.resources.capabilitystatement import CapabilityStatement, CapabilityStatementRest, \
    CapabilityStatementRestResource, CapabilityStatementRestResourceInteraction, \
    CapabilityStatementRestResourceOperation, CapabilityStatementRestResourceSearchParam
from fhir.resources.codesystem import CodeSystem
from fhir.resources.bundle import Bundle, BundleEntry
from fhir.resources.operationoutcome import OperationOutcome, OperationOutcomeIssue

from bioterms.etc.enums import ConceptPrefix
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status



fhir_router = APIRouter(
    prefix='/fhir',
    tags=['FHIR'],
)


@fhir_router.get('/metadata', response_model=CapabilityStatement)
async def get_fhir_metadata():
    statement = CapabilityStatement(
        status='active',
        date=datetime.now(tz=timezone.utc),
        kind='instance',
        fhirVersion='4.0.1',
        format=['json'],
        rest=[
            CapabilityStatementRest(
                mode='server',
                resource=[
                    CapabilityStatementRestResource(
                        type='CodeSystem',
                        interaction=[
                            CapabilityStatementRestResourceInteraction(code='read'),
                            CapabilityStatementRestResourceInteraction(code='search-type'),
                        ],
                        operation=[
                            CapabilityStatementRestResourceOperation(
                                name='lookup',
                                definition='http://hl7.org/fhir/OperationDefinition/CodeSystem-lookup',
                            ),
                            CapabilityStatementRestResourceOperation(
                                name='validate-code',
                                definition='http://hl7.org/fhir/OperationDefinition/CodeSystem-validate-code',
                            )
                        ],
                        searchParam=[
                            # URI search allows search with concept ID
                            CapabilityStatementRestResourceSearchParam(
                                name='url',
                                type='uri',
                                definition='http://hl7.org/fhir/SearchParameter/CanonicalResource-url',
                            ),
                            CapabilityStatementRestResourceSearchParam(
                                name='name',
                                type='string',
                                definition='http://hl7.org/fhir/SearchParameter/SearchParameter-name',
                            ),
                            CapabilityStatementRestResourceSearchParam(
                                name='description',
                                type='string',
                                definition='http://hl7.org/fhir/SearchParameter/SearchParameter-description',
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    return statement


@fhir_router.get('/CodeSystem', response_model=Bundle)
async def get_fhir_code_systems(request: Request,
                                doc_db: DocumentDatabase = Depends(get_active_doc_db),
                                graph_db: GraphDatabase = Depends(get_active_graph_db),
                                ):
    code_systems = []
    request_base_url = str(request.base_url).strip('/')

    for prefix in ConceptPrefix:
        vocab_status = await get_vocabulary_status(
            prefix,
            doc_db=doc_db,
            graph_db=graph_db,
        )

        if vocab_status.loaded:
            code_systems.append(CodeSystem(
                id=vocab_status.prefix.value,
                url=f'{request_base_url}/CodeSystem/{vocab_status.prefix.value}',
                name=vocab_status.name,
                title=vocab_status.name,
                status='active',
                content='fragment',
            ))

    bundle = Bundle(
        type='searchset',
        total=len(code_systems),
        entry=[
            BundleEntry(
                fullUrl=f'{request_base_url}/CodeSystem/{cs.id}',
                resource=cs,
            ) for cs in code_systems
        ],
    )

    return bundle


@fhir_router.get('/CodeSystem/{prefix}', response_model=CodeSystem)
async def get_fhir_code_system(prefix: ConceptPrefix,
                               request: Request,
                               doc_db: DocumentDatabase = Depends(get_active_doc_db),
                               graph_db: GraphDatabase = Depends(get_active_graph_db),
                               ):
    request_base_url = str(request.base_url).strip('/')
    vocab_status = await get_vocabulary_status(
        prefix,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    if not vocab_status.loaded:
        return OperationOutcome(
            issue=[
                OperationOutcomeIssue(
                    severity='error',
                    code='not-found',
                    diagnostics=f'CodeSystem/{prefix.value} not found'
                )
            ]
        )

    return CodeSystem(
        id=vocab_status.prefix.value,
        url=f'{request_base_url}/CodeSystem/{vocab_status.prefix.value}',
        name=vocab_status.name,
        title=vocab_status.name,
        status='active',
        content='fragment',
    )
