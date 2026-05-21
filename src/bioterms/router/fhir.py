from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from fhir.resources.capabilitystatement import CapabilityStatement, CapabilityStatementRest, \
    CapabilityStatementRestResource, CapabilityStatementRestResourceInteraction, \
    CapabilityStatementRestResourceOperation, CapabilityStatementRestResourceSearchParam
from fhir.resources.codesystem import CodeSystem
from fhir.resources.bundle import Bundle, BundleEntry
from fhir.resources.operationoutcome import OperationOutcome, OperationOutcomeIssue
from fhir.resources.parameters import Parameters, ParametersParameter

from bioterms.etc.consts import CONFIG
from bioterms.etc.enums import ConceptPrefix, ConceptStatus
from bioterms.model.concept import ConceptUnion
from bioterms.database import DocumentDatabase, GraphDatabase, get_active_doc_db, get_active_graph_db
from bioterms.vocabulary import get_vocabulary_status, get_vocabulary_config


fhir_router = APIRouter(
    prefix='/fhir',
    tags=['FHIR'],
)


def concept_to_parameters(concept: ConceptUnion,
                          properties: Optional[list[str]] = None,
                          ) -> Parameters:
    """
    Convert a Concept object to a FHIR Parameters object.
    :param concept: The object to convert, allows any class in ConceptUnion
    :param properties: Optional properties list to control the included fields
    :return: The FHIR Parameters object.
    """
    base_url = CONFIG.fhir_canonical_url.strip('/')
    name_param = ParametersParameter(
        name='name',
        valueString=concept.prefix.value,
    )
    code_param = ParametersParameter(
        name='code',
        valueCode=concept.concept_id,
    )
    system_param = ParametersParameter(
        name='system',
        valueUri=f'{base_url}/CodeSystem/{concept.prefix.value}',
    )
    display_param = ParametersParameter(
        name='display',
        valueString=concept.label,
    ) if concept.label else None
    deprecated_param = ParametersParameter(
        name='property',
        part=[
            ParametersParameter(
                name='code',
                valueCode='inactive',
            ),
            ParametersParameter(
                name='value',
                valueBoolean=concept.status == ConceptStatus.DEPRECATED,
            ),
        ]
    )
    synonym_params = [
        ParametersParameter(
            name='designation',
            part=[
                ParametersParameter(
                    name='value',
                    valueString=s,
                )
            ],
        ) for s in concept.synonyms
    ] if concept.synonyms else None
    definition_param = ParametersParameter(
        name='definition',
        valueString=concept.definition,
    ) if concept.definition else None

    if properties:
        parameters = []
        if 'name' in properties:
            parameters.append(name_param)
        if 'code' in properties:
            parameters.append(code_param)
        if 'system' in properties:
            parameters.append(system_param)
        if 'display' in properties and display_param:
            parameters.append(display_param)
        if 'inactive' in properties:
            parameters.append(deprecated_param)
        if 'designation' in properties and synonym_params:
            parameters.extend(synonym_params)
        if 'definition' in properties and definition_param:
            parameters.append(definition_param)

        return Parameters(parameter=parameters)

    all_params = [
        name_param,
        code_param,
        system_param,
        deprecated_param
    ]
    if display_param:
        all_params.append(display_param)
    if synonym_params:
        all_params.extend(synonym_params)
    if definition_param:
        all_params.append(definition_param)

    return Parameters(parameter=all_params)


async def _lookup_fhir_code(base_url: str,
                            system: str,
                            code: str,
                            doc_db: DocumentDatabase,
                            properties: Optional[list[str]] = None,
                            ):
    if not system.startswith(base_url):
        return JSONResponse(
            status_code=422,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='invalid',
                        diagnostics=f'Invalid code system: {system}'
                    )
                ]
            ).model_dump(),
        )

    try:
        prefix = ConceptPrefix(system[len(base_url):])
    except ValueError:
        return JSONResponse(
            status_code=404,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='not-found',
                        diagnostics=f'Code system not found: {system}'
                    )
                ]
            ).model_dump(),
        )

    vocab_config = get_vocabulary_config(prefix)
    concepts = await doc_db.get_terms_by_ids(
        prefix=prefix,
        concept_ids=[code],
        model_class=vocab_config['conceptClass'],
    )
    if not concepts:
        return JSONResponse(
            status_code=404,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='not-found',
                        diagnostics=f'Concept not found: {code}'
                    )
                ]
            ).model_dump(),
        )

    concept = concepts[0]

    return concept_to_parameters(concept, properties)


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
async def get_fhir_code_systems(doc_db: DocumentDatabase = Depends(get_active_doc_db),
                                graph_db: GraphDatabase = Depends(get_active_graph_db),
                                ):
    code_systems = []
    base_url = CONFIG.fhir_canonical_url.strip('/')

    for prefix in ConceptPrefix:
        vocab_status = await get_vocabulary_status(
            prefix,
            doc_db=doc_db,
            graph_db=graph_db,
        )

        if vocab_status.loaded:
            code_systems.append(CodeSystem(
                id=vocab_status.prefix.value,
                url=f'{base_url}/CodeSystem/{vocab_status.prefix.value}',
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
                fullUrl=f'{base_url}/CodeSystem/{cs.id}',
                resource=cs,
            ) for cs in code_systems
        ],
    )

    return bundle


@fhir_router.get(
    '/CodeSystem/{prefix}',
    response_model=CodeSystem,
    responses={
        404: {'model': OperationOutcome}
    }
)
async def get_fhir_code_system(prefix: ConceptPrefix,
                               doc_db: DocumentDatabase = Depends(get_active_doc_db),
                               graph_db: GraphDatabase = Depends(get_active_graph_db),
                               ):
    base_url = CONFIG.fhir_canonical_url.strip('/')
    vocab_status = await get_vocabulary_status(
        prefix,
        doc_db=doc_db,
        graph_db=graph_db,
    )

    if not vocab_status.loaded:
        return JSONResponse(
            status_code=404,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='not-found',
                        diagnostics=f'CodeSystem/{prefix.value} not found'
                    )
                ]
            ).model_dump(),
        )

    return CodeSystem(
        id=vocab_status.prefix.value,
        url=f'{base_url}/CodeSystem/{vocab_status.prefix.value}',
        name=vocab_status.name,
        title=vocab_status.name,
        status='active',
        content='fragment',
    )


@fhir_router.get(
    '/CodeSystem/$lookup',
    response_model=Parameters,
    responses={
        404: {'model': OperationOutcome},
        422: {'model': OperationOutcome},
    }
)
async def lookup_fhir_code(system: str = Query(..., description='The code system to lookup in.'),
                           code: str = Query(..., description='The code to lookup.'),
                           property: Optional[list[str]] = Query(None, description='The properties to return.'),
                           doc_db: DocumentDatabase = Depends(get_active_doc_db),
                           ):
    base_url = CONFIG.fhir_canonical_url.strip('/')
    return await _lookup_fhir_code(
        base_url=base_url,
        system=system,
        code=code,
        doc_db=doc_db,
        properties=property,
    )


@fhir_router.post(
    '/CodeSystem/$lookup',
    response_model=Parameters,
    responses={
        404: {'model': OperationOutcome},
        422: {'model': OperationOutcome},
    }
)
async def lookup_fhir_code_post(search_params: Parameters,
                                doc_db: DocumentDatabase = Depends(get_active_doc_db),
                                ):
    base_url = CONFIG.fhir_canonical_url.strip('/')
    coding = next((
        p for p in (search_params.parameter or [])
        if p.name == 'coding' and p.valueCoding is not None
    ), None)
    code = next((
        p for p in (search_params.parameter or [])
        if p.name == 'code' and p.valueCode is not None
    ))
    system = next((
        p for p in (search_params.parameter or [])
        if p.name == 'system' and p.valueCode is not None
    ))

    if coding and (code or system):
        return JSONResponse(
            status_code=422,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='invalid',
                        diagnostics='Cannot use both coding and code/system'
                    )
                ]
            ).model_dump(),
        )

    if not coding and not (code and system):
        return JSONResponse(
            status_code=422,
            content=OperationOutcome(
                issue=[
                    OperationOutcomeIssue(
                        severity='error',
                        code='invalid',
                        diagnostics='Must provide coding or code and system'
                    )
                ]
            )
        )

    if coding:
        code_input = coding.valueCoding.code
        system_input = coding.valueCoding.system
    else:
        code_input = code.valueCode
        system_input = system.valueUri

    return await _lookup_fhir_code(
        base_url=base_url,
        system=system_input,
        code=code_input,
        doc_db=doc_db,
    )
