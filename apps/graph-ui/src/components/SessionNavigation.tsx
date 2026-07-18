import { Search, ShieldCheck, Sparkles } from 'lucide-react'
import { useMemo, useState } from 'react'
import type { SessionSummary } from '../api/contracts'

interface SessionNavigationProps {
  sessions: readonly SessionSummary[]
  selectedSessionId: SessionSummary['session_id'] | null
  onSelect: (sessionId: SessionSummary['session_id']) => void
}

function outcomeLabel(outcome: SessionSummary['outcome']): string {
  return outcome.replaceAll('_', ' ')
}

export function SessionNavigation({
  sessions,
  selectedSessionId,
  onSelect,
}: SessionNavigationProps) {
  const [query, setQuery] = useState('')
  const filteredSessions = useMemo(() => {
    const normalized = query.trim().toLocaleLowerCase()
    if (normalized.length === 0) {
      return sessions
    }
    return sessions.filter((session) =>
      `${session.display.title} ${session.display.summary} ${session.outcome}`
        .toLocaleLowerCase()
        .includes(normalized),
    )
  }, [query, sessions])

  return (
    <aside
      className="sticky top-[4.5rem] flex h-[calc(100dvh-4.5rem)] w-[22rem] shrink-0 flex-col border-r border-white/10 bg-ink px-4 py-6 text-paper max-lg:relative max-lg:top-0 max-lg:h-auto max-lg:w-full max-lg:border-r-0 max-lg:border-b max-lg:border-white/10"
      aria-label="Experience sessions"
    >
      <div className="mb-5 flex items-end justify-between px-2">
        <div>
          <p className="mb-1 text-[0.65rem] font-extrabold tracking-[0.16em] text-mint/65 uppercase">Experience library</p>
          <h2 className="font-display text-2xl tracking-[-0.02em]">Sessions</h2>
        </div>
        <span
          className="rounded-full border border-white/10 bg-white/7 px-3 py-1 text-xs font-bold text-mint"
          aria-label={`${sessions.length} sessions`}
        >
          {sessions.length}
        </span>
      </div>

      <label className="mb-5 flex items-center gap-2 rounded-xl border border-white/10 bg-white/6 px-3.5 py-2.5 text-mint/60 transition focus-within:border-mint/40 focus-within:bg-white/9">
        <Search aria-hidden="true" size={17} />
        <span className="sr-only">Filter sessions</span>
        <input
          type="search"
          placeholder="Filter by meaning or outcome"
          className="min-w-0 flex-1 border-0 bg-transparent text-sm text-paper outline-none placeholder:text-paper/35"
          value={query}
          onChange={(event) => setQuery(event.currentTarget.value)}
        />
      </label>

      <nav
        className="flex min-h-0 flex-1 flex-col gap-2 overflow-y-auto pr-1 max-lg:max-h-[20rem] max-lg:flex-row max-lg:overflow-x-auto max-lg:overflow-y-hidden max-lg:pb-2"
        aria-label="Session results"
      >
        {filteredSessions.map((session) => {
          const isSelected = session.session_id === selectedSessionId
          const isEnriched = session.enrichment.state === 'completed'
          const SourceIcon = isEnriched ? Sparkles : ShieldCheck
          return (
            <button
              className="group flex w-full shrink-0 flex-col gap-2 rounded-2xl border border-white/8 bg-white/[0.035] p-4 text-left text-paper transition duration-200 hover:-translate-y-0.5 hover:border-white/16 hover:bg-white/[0.07] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-mint data-[selected=true]:border-mint/40 data-[selected=true]:bg-mint/[0.12] data-[selected=true]:shadow-[inset_3px_0_0_#b6dfca] max-lg:w-[19rem]"
              data-selected={isSelected}
              data-session-id={session.session_id}
              key={session.session_id}
              type="button"
              onClick={() => onSelect(session.session_id)}
              aria-current={isSelected ? 'page' : undefined}
            >
              <div className="flex w-full items-center justify-between">
                <span
                  className={
                    isEnriched
                      ? 'inline-flex items-center gap-1.5 text-[0.63rem] font-extrabold tracking-[0.08em] text-mint uppercase'
                      : 'inline-flex items-center gap-1.5 text-[0.63rem] font-extrabold tracking-[0.08em] text-paper/50 uppercase'
                  }
                >
                  <SourceIcon aria-hidden="true" size={13} />
                  {isEnriched ? 'Enriched' : 'Deterministic'}
                </span>
                <span
                  className={`h-2 w-2 rounded-full ${
                    session.outcome === 'verified_success'
                      ? 'bg-emerald-400'
                      : session.outcome === 'failed'
                        ? 'bg-coral'
                        : session.outcome === 'cancelled'
                          ? 'bg-slate-400'
                          : 'bg-gold'
                  }`}
                  aria-hidden="true"
                />
              </div>
              <strong className="line-clamp-2 text-[0.94rem] leading-snug tracking-[-0.01em]">{session.display.title}</strong>
              <span className="line-clamp-2 text-xs leading-relaxed text-paper/52">{session.display.summary}</span>
              <span className="mt-1 text-[0.67rem] font-semibold text-paper/38 capitalize">
                {session.activity_count.toLocaleString()} activities · {outcomeLabel(session.outcome)}
              </span>
            </button>
          )
        })}
        {filteredSessions.length === 0 ? (
          <p className="rounded-xl border border-dashed border-white/10 px-4 py-8 text-center text-sm text-paper/45">
            No sessions match this filter.
          </p>
        ) : null}
      </nav>
    </aside>
  )
}
