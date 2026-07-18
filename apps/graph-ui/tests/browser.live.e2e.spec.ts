import { expect, test } from '@playwright/test'
import type { APIRequestContext, Page } from '@playwright/test'
import {
  sessionDetailResponseSchema,
  sessionListResponseSchema,
} from '../src/api/contracts'
import type { SessionDetail, SessionSummary } from '../src/api/contracts'

const configuredApiOrigin = process.env.VITE_API_PROXY_TARGET ?? 'http://127.0.0.1:3000'
const configuredApiUrl = new URL(configuredApiOrigin)
if (
  configuredApiUrl.protocol !== 'http:' ||
  !['127.0.0.1', 'localhost', '::1'].includes(configuredApiUrl.hostname) ||
  configuredApiUrl.username !== '' ||
  configuredApiUrl.password !== '' ||
  configuredApiUrl.pathname !== '/'
) {
  throw new Error('VITE_API_PROXY_TARGET for live E2E must be a credential-free local HTTP origin.')
}
const LIVE_API_ORIGIN = configuredApiUrl.origin
const MAX_COMPLETED_CANDIDATES = 50

async function readLiveJson(request: APIRequestContext, path: string): Promise<unknown> {
  let response
  try {
    response = await request.get(`${LIVE_API_ORIGIN}${path}`, {
      headers: { Accept: 'application/json' },
      timeout: 10_000,
    })
  } catch {
    throw new Error(
      `Live Rust API is unavailable at ${LIVE_API_ORIGIN}. Start harness-graph serve with real Neo4j before running test:e2e:live.`,
    )
  }

  if (!response.ok()) {
    throw new Error(
      `Live Rust API request ${path} returned HTTP ${response.status()}; the live browser E2E requires a healthy API and Neo4j.`,
    )
  }

  try {
    return await response.json()
  } catch {
    throw new Error(`Live Rust API request ${path} did not return valid JSON.`)
  }
}

function assertNoPrivateSurface(payload: unknown): void {
  const serialized = JSON.stringify(payload)
  expect(serialized).not.toMatch(/"key"\s*:/)
  expect(serialized).not.toContain('raw_transcript')
  expect(serialized).not.toContain('transcript_text')
  expect(serialized).not.toContain('field_path')
  expect(serialized).not.toMatch(/(?:\/Users\/|\/home\/|file:\/\/|[A-Za-z]:\\Users\\)/)
  expect(serialized).not.toContain('MISTRAL_API_KEY')
}

async function readLiveDetail(
  request: APIRequestContext,
  summary: SessionSummary,
): Promise<SessionDetail> {
  const payload = await readLiveJson(
    request,
    `/v1/experience/sessions/${encodeURIComponent(summary.session_id)}`,
  )
  assertNoPrivateSurface(payload)
  const decoded = sessionDetailResponseSchema.safeParse(payload)
  if (!decoded.success) {
    throw new Error('Live Rust API session detail violated the source-safe UI contract.')
  }
  if (decoded.data.session_id !== summary.session_id) {
    throw new Error('Live Rust API returned a session detail with a mismatched identity.')
  }
  return decoded.data
}

async function selectSession(page: Page, summary: SessionSummary): Promise<void> {
  const candidate = page.locator(`button[data-session-id="${summary.session_id}"]`)
  await expect(candidate).toBeVisible()
  await candidate.click()
  await expect(page.getByRole('heading', { level: 1, name: summary.display.title })).toBeVisible()
}

function firstCitation(detail: SessionDetail): string | null {
  if (detail.enrichment.state !== 'completed') {
    return null
  }
  return (
    detail.enrichment.episodes[0]?.citations[0]?.anchor_id ??
    detail.enrichment.claims[0]?.citations[0]?.anchor_id ??
    detail.enrichment.relations[0]?.citations[0]?.anchor_id ??
    null
  )
}

test('real Rust API and Neo4j drive enriched and deterministic browser views', async ({
  page,
  request,
}, testInfo) => {
  const listPayload = await readLiveJson(request, '/v1/experience/sessions')
  assertNoPrivateSurface(listPayload)
  const list = sessionListResponseSchema.safeParse(listPayload)
  if (!list.success) {
    throw new Error('Live Rust API session list violated the source-safe UI contract.')
  }
  if (list.data.sessions.length === 0) {
    throw new Error('Live Neo4j returned no experience sessions; import verified sessions first.')
  }

  const fallback = list.data.sessions.find(
    (session) =>
      session.display.source === 'deterministic_fallback' &&
      session.enrichment.state === 'unavailable',
  )
  if (fallback === undefined) {
    throw new Error(
      'Live Neo4j contains no deterministic-fallback session; at least one session without a selected completed enrichment is required.',
    )
  }
  const fallbackDetail = await readLiveDetail(request, fallback)
  if (fallbackDetail.enrichment.state !== 'unavailable') {
    throw new Error('Live list/detail enrichment state disagrees for the fallback session.')
  }

  await page.goto('/')
  await expect(page.getByRole('navigation', { name: 'Session results' })).toBeVisible()
  await selectSession(page, fallback)
  await expect(page.getByRole('heading', { name: 'Deterministic view is active' })).toBeVisible()
  await expect(page.getByText('Mistral semantic overlay', { exact: true })).toHaveCount(0)
  expect(await page.locator('body').innerText()).not.toMatch(
    /(?:raw_transcript|transcript_text|field_path|MISTRAL_API_KEY|\/Users\/|\/home\/)/,
  )

  const completedSummaries = list.data.sessions
    .filter((session) => session.enrichment.state === 'completed')
    .slice(0, MAX_COMPLETED_CANDIDATES)
  if (completedSummaries.length === 0) {
    testInfo.annotations.push({
      type: 'enrichment',
      description: 'No completed enrichment exists yet; deterministic live flow was validated.',
    })
    return
  }

  let citedCompleted: { summary: SessionSummary; detail: SessionDetail; anchorId: string } | null =
    null
  for (const summary of completedSummaries) {
    const detail = await readLiveDetail(request, summary)
    const anchorId = firstCitation(detail)
    if (anchorId !== null) {
      citedCompleted = { summary, detail, anchorId }
      break
    }
  }
  if (citedCompleted === null || citedCompleted.detail.enrichment.state !== 'completed') {
    throw new Error(
      `Live Neo4j has completed enrichment but none of the first ${MAX_COMPLETED_CANDIDATES} completed sessions exposes a resolvable citation.`,
    )
  }

  await selectSession(page, citedCompleted.summary)
  await expect(page.getByText('Mistral semantic overlay', { exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Versioned semantic overlay' })).toBeVisible()
  await expect(
    page.getByText(citedCompleted.detail.enrichment.model, { exact: true }),
  ).toBeVisible()

  const anchor = citedCompleted.detail.source_anchors.find(
    (candidate) => candidate.anchor_id === citedCompleted.anchorId,
  )
  if (anchor === undefined) {
    throw new Error('Live completed enrichment citation has no source-anchor projection.')
  }
  const citationLink = page.locator(`a[href="#source-${citedCompleted.anchorId}"]`).first()
  await expect(citationLink).toBeVisible()
  await citationLink.click()
  await expect(page).toHaveURL(new RegExp(`#source-${citedCompleted.anchorId}$`))
  await expect(page.getByRole('heading', { name: anchor.label })).toBeVisible()
  expect(await page.locator('body').innerText()).not.toMatch(
    /(?:raw_transcript|transcript_text|field_path|MISTRAL_API_KEY|\/Users\/|\/home\/)/,
  )
})
