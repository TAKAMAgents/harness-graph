import { sessionDetailResponseSchema, sessionListResponseSchema } from './contracts'
import type { SessionDetail, SessionListResponse, SessionSummary } from './contracts'

export class ExperienceApiError extends Error {
  readonly category: 'transport' | 'response' | 'contract'

  constructor(category: 'transport' | 'response' | 'contract', message: string) {
    super(message)
    this.name = 'ExperienceApiError'
    this.category = category
  }
}

async function requestJson(path: string, signal: AbortSignal): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(path, {
      signal,
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    })
  } catch (error: unknown) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw error
    }
    throw new ExperienceApiError('transport', 'The experience API is unreachable.')
  }

  if (!response.ok) {
    throw new ExperienceApiError('response', `The experience API returned status ${response.status}.`)
  }

  try {
    return await response.json()
  } catch {
    throw new ExperienceApiError('contract', 'The experience API returned invalid JSON.')
  }
}

export async function loadSessions(signal: AbortSignal): Promise<SessionListResponse> {
  const decoded = sessionListResponseSchema.safeParse(
    await requestJson('/v1/experience/sessions', signal),
  )
  if (!decoded.success) {
    throw new ExperienceApiError('contract', 'The session list violated the source-safe API contract.')
  }
  return decoded.data
}

export async function loadSessionDetail(
  sessionId: SessionSummary['session_id'],
  signal: AbortSignal,
): Promise<SessionDetail> {
  const decoded = sessionDetailResponseSchema.safeParse(
    await requestJson(`/v1/experience/sessions/${encodeURIComponent(sessionId)}`, signal),
  )
  if (!decoded.success) {
    throw new ExperienceApiError('contract', 'The session detail violated the source-safe API contract.')
  }
  return decoded.data
}
