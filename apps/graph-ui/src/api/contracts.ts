import { z } from 'zod'

const SAFE_IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/
const FORBIDDEN_DISPLAY_PATTERNS = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/i,
  /\bBearer\s+[A-Za-z0-9._~+/=-]{12,}/i,
  /\b(?:api[_-]?key|access[_-]?token|password|secret)\s*[:=]\s*\S+/i,
  /\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/,
  /(?:^|\s)(?:\/Users\/|\/home\/|file:\/\/|[A-Za-z]:\\Users\\)/,
] as const

const sourceSafeText = (maximum: number) =>
  z
    .string()
    .trim()
    .min(1)
    .max(maximum)
    .refine(
      (value) => FORBIDDEN_DISPLAY_PATTERNS.every((pattern) => !pattern.test(value)),
      'response contained a forbidden secret or local-path pattern',
    )

const identitySchema = z.string().regex(SAFE_IDENTIFIER).brand<'SafeIdentity'>()
const digestSchema = z.string().regex(/^[a-f0-9]{64}$/).brand<'ContentDigest'>()
const displaySourceSchema = z.enum(['enrichment', 'deterministic_fallback'])
const outcomeSchema = z.enum([
  'verified_success',
  'unverified_completion',
  'failed',
  'inconclusive',
  'cancelled',
])
const confidenceSchema = z.enum(['low', 'medium', 'high'])
const epistemicStatusSchema = z.enum(['explicit', 'inferred', 'hypothesis'])
const activityStatusSchema = z.enum([
  'pending',
  'succeeded',
  'failed',
  'interrupted',
  'indeterminate',
])

const displaySchema = z
  .object({
    source: displaySourceSchema,
    title: sourceSafeText(160),
    summary: sourceSafeText(1_000),
  })
  .strict()

const unavailableEnrichmentSchema = z
  .object({
    state: z.literal('unavailable'),
    reason: z.enum(['disabled', 'not_eligible', 'no_completed_run', 'failed_or_partial']),
  })
  .strict()

const completedSummaryEnrichmentSchema = z
  .object({
    state: z.literal('completed'),
    run_id: digestSchema,
    confidence: confidenceSchema.optional(),
    epistemic_status: epistemicStatusSchema.optional(),
  })
  .strict()

export const sessionSummarySchema = z
  .object({
    session_id: identitySchema,
    display: displaySchema,
    outcome: outcomeSchema,
    activity_count: z.number().int().nonnegative().max(10_000_000),
    enrichment: z.discriminatedUnion('state', [
      completedSummaryEnrichmentSchema,
      unavailableEnrichmentSchema,
    ]),
  })
  .strict()
  .superRefine((value, context) => {
    const expectedSource = value.enrichment.state === 'completed' ? 'enrichment' : 'deterministic_fallback'
    if (value.display.source !== expectedSource) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'display source does not match enrichment lifecycle',
        path: ['display', 'source'],
      })
    }
  })

export const sessionListResponseSchema = z
  .object({
    sessions: z.array(sessionSummarySchema).max(100_000),
  })
  .strict()

const activitySchema = z
  .object({
    activity_id: identitySchema,
    sequence: z.number().int().positive().max(10_000_000),
    label: sourceSafeText(160),
    status: activityStatusSchema,
  })
  .strict()

const citationSchema = z
  .object({
    anchor_id: digestSchema,
  })
  .strict()

const episodeSchema = z
  .object({
    episode_id: digestSchema,
    ordinal: z.number().int().positive().max(10_000),
    title: sourceSafeText(160),
    summary: sourceSafeText(1_000),
    confidence: confidenceSchema,
    epistemic_status: epistemicStatusSchema,
    activity_ids: z.array(identitySchema).max(10_000),
    citations: z.array(citationSchema).min(1).max(10_000),
  })
  .strict()

const entityKindSchema = z.enum([
  'project',
  'repository',
  'module',
  'file',
  'command',
  'tool',
  'dependency',
  'configuration',
  'environment',
  'error',
  'concept',
  'artifact',
  'other',
])

const entitySchema = z
  .object({
    entity_id: digestSchema,
    kind: entityKindSchema,
    name: sourceSafeText(160),
  })
  .strict()

const knowledgeKindSchema = z.enum([
  'goal',
  'decision',
  'constraint',
  'artifact',
  'dependency',
  'failure',
  'root_cause_hypothesis',
  'repair',
  'verification',
  'risk',
  'lesson',
  'open_question',
])

const claimSubjectsSchema = z.discriminatedUnion('scope', [
  z.object({ scope: z.literal('session_wide') }).strict(),
  z
    .object({
      scope: z.literal('entities'),
      entity_ids: z.array(digestSchema).min(1).max(1_000),
    })
    .strict(),
])

const claimSchema = z
  .object({
    claim_id: digestSchema,
    kind: knowledgeKindSchema,
    title: sourceSafeText(160),
    statement: sourceSafeText(1_000),
    confidence: confidenceSchema,
    epistemic_status: epistemicStatusSchema,
    subjects: claimSubjectsSchema,
    citations: z.array(citationSchema).min(1).max(10_000),
  })
  .strict()

const predicateSchema = z.enum([
  'uses',
  'modifies',
  'depends_on',
  'causes',
  'blocked_by',
  'resolves',
  'verifies',
  'produces',
  'contributes_to',
  'contradicts',
  'related_to',
])

const relationSchema = z
  .object({
    relation_id: digestSchema,
    predicate: predicateSchema,
    subject_entity_id: digestSchema,
    object_entity_id: digestSchema,
    confidence: confidenceSchema,
    epistemic_status: epistemicStatusSchema,
    citations: z.array(citationSchema).min(1).max(10_000),
  })
  .strict()

const completedDetailEnrichmentSchema = z
  .object({
    state: z.literal('completed'),
    run_id: digestSchema,
    provider: z.literal('mistral'),
    model: identitySchema,
    prompt_version: identitySchema,
    disclosure_scope: z.enum(['conversation_only', 'conversation_and_execution']),
    authorization_policy_digest: digestSchema,
    prompt_digest: digestSchema,
    schema_version: identitySchema,
    confidence: confidenceSchema.optional(),
    epistemic_status: epistemicStatusSchema.optional(),
    episodes: z.array(episodeSchema).max(10_000),
    entities: z.array(entitySchema).max(100_000),
    claims: z.array(claimSchema).max(100_000),
    relations: z.array(relationSchema).max(100_000),
  })
  .strict()

const sourceAnchorSchema = z
  .object({
    anchor_id: digestSchema,
    label: sourceSafeText(160),
    source_kind: z.enum(['conversation', 'tool_request', 'tool_result', 'execution', 'verification']),
    record_sequence: z.number().int().positive().max(10_000_000),
    content_digest: digestSchema,
  })
  .strict()

export const sessionDetailResponseSchema = z
  .object({
    session_id: identitySchema,
    display: displaySchema,
    outcome: outcomeSchema,
    activities: z.array(activitySchema).max(1_000_000),
    enrichment: z.discriminatedUnion('state', [
      completedDetailEnrichmentSchema,
      unavailableEnrichmentSchema,
    ]),
    source_anchors: z.array(sourceAnchorSchema).max(100_000),
  })
  .strict()
  .superRefine((value, context) => {
    const knownAnchors = new Set(value.source_anchors.map((anchor) => anchor.anchor_id))
    const activityIds = new Set(value.activities.map((activity) => activity.activity_id))
    if (knownAnchors.size !== value.source_anchors.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'source anchors contain duplicate identities',
        path: ['source_anchors'],
      })
    }
    if (activityIds.size !== value.activities.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'activities contain duplicate identities',
        path: ['activities'],
      })
    }
    if (value.enrichment.state !== 'completed') {
      if (value.display.source !== 'deterministic_fallback') {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'unavailable enrichment requires deterministic fallback display',
          path: ['display', 'source'],
        })
      }
      return
    }
    if (value.display.source !== 'enrichment') {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'completed enrichment requires enrichment display',
        path: ['display', 'source'],
      })
    }
    const entityIds = new Set(value.enrichment.entities.map((entity) => entity.entity_id))
    if (entityIds.size !== value.enrichment.entities.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'entities contain duplicate identities',
        path: ['enrichment', 'entities'],
      })
    }
    for (const episode of value.enrichment.episodes) {
      if (episode.activity_ids.some((activityId) => !activityIds.has(activityId))) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'episode references an unresolved activity',
          path: ['enrichment', 'episodes'],
        })
      }
    }
    for (const claim of value.enrichment.claims) {
      if (
        claim.subjects.scope === 'entities' &&
        claim.subjects.entity_ids.some((entityId) => !entityIds.has(entityId))
      ) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'claim references an unresolved entity',
          path: ['enrichment', 'claims'],
        })
      }
    }
    for (const relation of value.enrichment.relations) {
      if (
        !entityIds.has(relation.subject_entity_id) ||
        !entityIds.has(relation.object_entity_id) ||
        relation.subject_entity_id === relation.object_entity_id
      ) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'relation endpoints are unresolved or identical',
          path: ['enrichment', 'relations'],
        })
      }
    }
    const citationIds = [
      ...value.enrichment.episodes.flatMap((episode) => episode.citations),
      ...value.enrichment.claims.flatMap((claim) => claim.citations),
      ...value.enrichment.relations.flatMap((relation) => relation.citations),
    ].map((citation) => citation.anchor_id)

    for (const anchorId of citationIds) {
      if (!knownAnchors.has(anchorId)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'citation references an unresolved source anchor',
          path: ['source_anchors'],
        })
      }
    }
  })

export type SessionSummary = z.infer<typeof sessionSummarySchema>
export type SessionListResponse = z.infer<typeof sessionListResponseSchema>
export type SessionDetail = z.infer<typeof sessionDetailResponseSchema>
export type Confidence = z.infer<typeof confidenceSchema>
export type EpistemicStatus = z.infer<typeof epistemicStatusSchema>
