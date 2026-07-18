const ENRICHED_RUN_ID = '1'.repeat(64)
const EPISODE_ONE_ID = '2'.repeat(64)
const EPISODE_TWO_ID = '3'.repeat(64)
const PROJECT_ENTITY_ID = '4'.repeat(64)
const GRAPH_ENTITY_ID = '5'.repeat(64)
const CLAIM_ONE_ID = '6'.repeat(64)
const CLAIM_TWO_ID = '7'.repeat(64)
const RELATION_ID = '8'.repeat(64)
const SOURCE_ONE_ID = 'a'.repeat(64)
const SOURCE_TWO_ID = 'b'.repeat(64)
const CONTENT_ONE_DIGEST = 'c'.repeat(64)
const CONTENT_TWO_DIGEST = 'd'.repeat(64)

const enrichedDisplay = {
  source: 'enrichment',
  title: 'Turning verified execution into reusable graph knowledge',
  summary:
    'The session preserved deterministic evidence while adding citation-backed entities, claims, and relations as a versioned semantic overlay.',
}

const deterministicDisplay = {
  source: 'deterministic_fallback',
  title: 'Inspect → modify → verify',
  summary:
    'A verified deterministic activity sequence is available; no completed semantic enrichment is selected.',
}

export const sessionListResponse = {
  sessions: [
    {
      session_id: 'ses_enriched_e2e',
      display: enrichedDisplay,
      outcome: 'verified_success',
      activity_count: 4,
      enrichment: {
        state: 'completed',
        run_id: ENRICHED_RUN_ID,
        confidence: 'high',
        epistemic_status: 'explicit',
      },
    },
    {
      session_id: 'ses_deterministic_e2e',
      display: deterministicDisplay,
      outcome: 'unverified_completion',
      activity_count: 3,
      enrichment: {
        state: 'unavailable',
        reason: 'no_completed_run',
      },
    },
  ],
}

export const sessionDetails = new Map([
  [
    'ses_enriched_e2e',
    {
      session_id: 'ses_enriched_e2e',
      display: enrichedDisplay,
      outcome: 'verified_success',
      activities: [
        { activity_id: 'act_inspect', sequence: 1, label: 'Inspect archive contract', status: 'succeeded' },
        { activity_id: 'act_design', sequence: 2, label: 'Design additive overlay', status: 'succeeded' },
        { activity_id: 'act_project', sequence: 3, label: 'Project cited knowledge', status: 'succeeded' },
        { activity_id: 'act_verify', sequence: 4, label: 'Verify base graph invariance', status: 'succeeded' },
      ],
      enrichment: {
        state: 'completed',
        run_id: ENRICHED_RUN_ID,
        provider: 'mistral',
        model: 'mistral-small-2603',
        prompt_version: 'transcript-knowledge-v1',
        schema_version: 'knowledge-overlay-v1',
        confidence: 'high',
        epistemic_status: 'explicit',
        episodes: [
          {
            episode_id: EPISODE_ONE_ID,
            ordinal: 1,
            title: 'Establish the evidence boundary',
            summary:
              'The execution verifies canonical source records and separates deterministic facts from model interpretation.',
            confidence: 'high',
            epistemic_status: 'explicit',
            activity_ids: ['act_inspect', 'act_design'],
            citations: [{ anchor_id: SOURCE_ONE_ID }],
          },
          {
            episode_id: EPISODE_TWO_ID,
            ordinal: 2,
            title: 'Project and verify the overlay',
            summary:
              'Validated claims are projected into an additive graph, then checked against the unchanged authoritative layer.',
            confidence: 'medium',
            epistemic_status: 'inferred',
            activity_ids: [],
            citations: [{ anchor_id: SOURCE_TWO_ID }],
          },
        ],
        entities: [
          { entity_id: PROJECT_ENTITY_ID, kind: 'project', name: 'HarnessGraph' },
          { entity_id: GRAPH_ENTITY_ID, kind: 'concept', name: 'Additive enrichment overlay' },
        ],
        claims: [
          {
            claim_id: CLAIM_ONE_ID,
            kind: 'decision',
            title: 'Keep deterministic extraction authoritative',
            statement:
              'Semantic knowledge is added as a parallel versioned layer and cannot replace verified graph facts.',
            confidence: 'high',
            epistemic_status: 'explicit',
            subjects: { scope: 'session_wide' },
            citations: [{ anchor_id: SOURCE_ONE_ID }],
          },
          {
            claim_id: CLAIM_TWO_ID,
            kind: 'verification',
            title: 'Resolve every semantic citation',
            statement:
              'Each displayed interpretation resolves to a content-free source anchor in the verified archive.',
            confidence: 'medium',
            epistemic_status: 'inferred',
            subjects: { scope: 'entities', entity_ids: [GRAPH_ENTITY_ID] },
            citations: [{ anchor_id: SOURCE_TWO_ID }],
          },
        ],
        relations: [
          {
            relation_id: RELATION_ID,
            predicate: 'produces',
            subject_entity_id: PROJECT_ENTITY_ID,
            object_entity_id: GRAPH_ENTITY_ID,
            confidence: 'high',
            epistemic_status: 'explicit',
            citations: [{ anchor_id: SOURCE_TWO_ID }],
          },
        ],
      },
      source_anchors: [
        {
          anchor_id: SOURCE_ONE_ID,
          label: 'Architecture decision evidence',
          source_kind: 'conversation',
          record_sequence: 18,
          content_digest: CONTENT_ONE_DIGEST,
        },
        {
          anchor_id: SOURCE_TWO_ID,
          label: 'Projection verification evidence',
          source_kind: 'verification',
          record_sequence: 41,
          content_digest: CONTENT_TWO_DIGEST,
        },
      ],
    },
  ],
  [
    'ses_deterministic_e2e',
    {
      session_id: 'ses_deterministic_e2e',
      display: deterministicDisplay,
      outcome: 'unverified_completion',
      activities: [
        { activity_id: 'act_fallback_inspect', sequence: 1, label: 'Inspect repository', status: 'succeeded' },
        { activity_id: 'act_fallback_modify', sequence: 2, label: 'Modify implementation', status: 'succeeded' },
        { activity_id: 'act_fallback_complete', sequence: 3, label: 'Complete without fresh verification', status: 'indeterminate' },
      ],
      enrichment: {
        state: 'unavailable',
        reason: 'no_completed_run',
      },
      source_anchors: [],
    },
  ],
])
