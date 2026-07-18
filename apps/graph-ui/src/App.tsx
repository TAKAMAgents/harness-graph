import { AlertTriangle, DatabaseZap, Network, RefreshCw } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import type { SessionDetail, SessionSummary } from './api/contracts'
import { loadSessionDetail, loadSessions } from './api/client'
import { SessionDetail as SessionDetailView } from './components/SessionDetail'
import { SessionNavigation } from './components/SessionNavigation'

type SessionListState =
  | { phase: 'loading' }
  | { phase: 'ready'; sessions: readonly SessionSummary[] }
  | { phase: 'failed'; message: string }

type SessionDetailState =
  | { phase: 'idle' }
  | { phase: 'loading'; sessionId: SessionSummary['session_id'] }
  | { phase: 'ready'; session: SessionDetail }
  | { phase: 'failed'; sessionId: SessionSummary['session_id']; message: string }

function sourceSafeErrorMessage(error: unknown): string {
  if (error instanceof Error && error.name === 'AbortError') {
    return 'Request cancelled.'
  }
  return error instanceof Error ? error.message : 'The experience view is unavailable.'
}

function selectedFromUrl(sessions: readonly SessionSummary[]): SessionSummary['session_id'] | null {
  const requested = new URLSearchParams(window.location.search).get('session')
  const matching = sessions.find((session) => session.session_id === requested)
  return matching?.session_id ?? sessions[0]?.session_id ?? null
}

function LoadingDetail() {
  return (
    <div className="animate-pulse rounded-[2rem] bg-paper p-8" aria-busy="true" aria-label="Loading session detail">
      <div className="mb-6 h-6 w-36 rounded-full bg-ink/8" />
      <div className="h-16 w-4/5 rounded-2xl bg-ink/8" />
      <div className="mt-6 h-4 w-full rounded-full bg-ink/7" />
      <div className="mt-3 h-4 w-3/5 rounded-full bg-ink/7" />
      <div className="mt-10 h-72 rounded-3xl bg-ink/7" />
    </div>
  )
}

interface FailurePanelProps {
  message: string
  onRetry: () => void
}

function FailurePanel({ message, onRetry }: FailurePanelProps) {
  return (
    <section className="mx-auto flex max-w-lg flex-col items-center rounded-3xl border border-red-900/10 bg-paper p-10 text-center shadow-[0_20px_60px_rgb(8_18_15_/_0.06)]" role="alert">
      <span className="mb-5 grid h-14 w-14 place-items-center rounded-2xl bg-red-100 text-red-700"><AlertTriangle aria-hidden="true" size={28} /></span>
      <h1 className="font-display text-3xl text-ink">Experience view unavailable</h1>
      <p className="mt-3 text-sm leading-relaxed text-ink/60">{message}</p>
      <button
        className="mt-6 inline-flex items-center gap-2 rounded-xl bg-pine px-4 py-2.5 text-sm font-bold text-white transition hover:bg-ink focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-pine"
        type="button"
        onClick={onRetry}
      >
        <RefreshCw aria-hidden="true" size={16} /> Retry
      </button>
    </section>
  )
}

export function App() {
  const [listState, setListState] = useState<SessionListState>({ phase: 'loading' })
  const [selectedSessionId, setSelectedSessionId] = useState<SessionSummary['session_id'] | null>(null)
  const [detailState, setDetailState] = useState<SessionDetailState>({ phase: 'idle' })
  const [reloadSequence, setReloadSequence] = useState(0)

  const fetchSessions = useCallback(() => setReloadSequence((value) => value + 1), [])

  useEffect(() => {
    const controller = new AbortController()
    setListState({ phase: 'loading' })
    void loadSessions(controller.signal)
      .then(({ sessions }) => {
        setListState({ phase: 'ready', sessions })
        setSelectedSessionId((current) => {
          if (current !== null && sessions.some((session) => session.session_id === current)) {
            return current
          }
          return selectedFromUrl(sessions)
        })
      })
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === 'AbortError')) {
          setListState({ phase: 'failed', message: sourceSafeErrorMessage(error) })
        }
      })
    return () => controller.abort()
  }, [reloadSequence])

  useEffect(() => {
    if (selectedSessionId === null) {
      setDetailState({ phase: 'idle' })
      return
    }
    const controller = new AbortController()
    setDetailState({ phase: 'loading', sessionId: selectedSessionId })
    void loadSessionDetail(selectedSessionId, controller.signal)
      .then((session) => setDetailState({ phase: 'ready', session }))
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === 'AbortError')) {
          setDetailState({
            phase: 'failed',
            sessionId: selectedSessionId,
            message: sourceSafeErrorMessage(error),
          })
        }
      })
    return () => controller.abort()
  }, [selectedSessionId, reloadSequence])

  useEffect(() => {
    const selectFromHistory = () => {
      if (listState.phase === 'ready') {
        setSelectedSessionId(selectedFromUrl(listState.sessions))
      }
    }
    window.addEventListener('popstate', selectFromHistory)
    return () => window.removeEventListener('popstate', selectFromHistory)
  }, [listState])

  const sessions = useMemo(
    () => (listState.phase === 'ready' ? listState.sessions : []),
    [listState],
  )

  const selectSession = useCallback((sessionId: SessionSummary['session_id']) => {
    const url = new URL(window.location.href)
    url.searchParams.set('session', sessionId)
    url.hash = ''
    window.history.pushState(null, '', url)
    setSelectedSessionId(sessionId)
    document.getElementById('main-content')?.focus()
  }, [])

  const retryDetail = useCallback(() => setReloadSequence((value) => value + 1), [])

  return (
    <div className="min-h-dvh">
      <header className="sticky top-0 z-50 flex h-[4.5rem] items-center justify-between border-b border-white/10 bg-ink/95 px-6 text-paper shadow-sm backdrop-blur-md max-sm:px-4">
        <a className="flex items-center gap-3 rounded-lg focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-mint" href="/" aria-label="HarnessGraph home">
          <span className="grid h-9 w-9 place-items-center rounded-xl bg-mint text-ink"><Network aria-hidden="true" size={21} /></span>
          <span><strong className="block text-sm tracking-[0.01em]">HarnessGraph</strong><small className="block text-[0.63rem] tracking-[0.08em] text-paper/45 uppercase">Experience explorer</small></span>
        </a>
        <div className="flex items-center gap-2 text-xs font-bold text-paper/55 max-sm:text-[0]">
          <span className="h-2 w-2 animate-[soft-pulse_2.2s_ease-in-out_infinite] rounded-full bg-emerald-400" aria-hidden="true" />
          Evidence-linked view
        </div>
      </header>

      {listState.phase === 'failed' ? (
        <main id="main-content" className="grid min-h-[calc(100dvh-4.5rem)] place-items-center p-6 outline-none" tabIndex={-1}>
          <FailurePanel message={listState.message} onRetry={fetchSessions} />
        </main>
      ) : (
        <div className="flex min-h-[calc(100dvh-4.5rem)] items-start max-lg:flex-col">
          {listState.phase === 'loading' ? (
            <aside className="sticky top-[4.5rem] h-[calc(100dvh-4.5rem)] w-[22rem] shrink-0 animate-pulse bg-ink p-6 max-lg:relative max-lg:top-0 max-lg:h-64 max-lg:w-full" aria-busy="true" aria-label="Loading sessions">
              <div className="h-5 w-2/5 rounded-full bg-white/10" />
              <div className="mt-8 h-28 rounded-2xl bg-white/8" />
              <div className="mt-3 h-28 rounded-2xl bg-white/8" />
            </aside>
          ) : (
            <SessionNavigation
              sessions={sessions}
              selectedSessionId={selectedSessionId}
              onSelect={selectSession}
            />
          )}

          <main id="main-content" className="min-w-0 flex-1 p-6 outline-none sm:p-8 xl:p-10" tabIndex={-1}>
            {detailState.phase === 'ready' ? <SessionDetailView session={detailState.session} /> : null}
            {detailState.phase === 'loading' ? <LoadingDetail /> : null}
            {detailState.phase === 'failed' ? (
              <FailurePanel message={detailState.message} onRetry={retryDetail} />
            ) : null}
            {detailState.phase === 'idle' && listState.phase === 'ready' && sessions.length === 0 ? (
              <section className="mx-auto flex max-w-lg flex-col items-center rounded-3xl bg-paper p-10 text-center">
                <span className="mb-5 grid h-14 w-14 place-items-center rounded-2xl bg-pine/8 text-pine"><DatabaseZap aria-hidden="true" size={30} /></span>
                <h1 className="font-display text-3xl text-ink">No verified sessions yet</h1>
                <p className="mt-3 text-sm leading-relaxed text-ink/60">Import a checksum-verified archive to populate the experience graph.</p>
              </section>
            ) : null}
          </main>
        </div>
      )}
    </div>
  )
}
