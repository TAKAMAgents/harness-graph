import {
  Activity,
  ArrowRight,
  BookOpen,
  Boxes,
  Braces,
  CircleCheckBig,
  GitBranch,
  Link2,
  Network,
  Quote,
  ShieldCheck,
  Sparkles,
} from 'lucide-react'
import type { ReactNode } from 'react'
import type { SessionDetail as SessionDetailDto } from '../api/contracts'
import { ConfidenceBadge, EpistemicBadge, VocabularyBadge } from './Badges'
import { CitationLinks } from './CitationLinks'

interface SessionDetailProps {
  session: SessionDetailDto
}

function displayLabel(value: string): string {
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

interface SectionHeadingProps {
  eyebrow: string
  title: string
  description: string
  icon: ReactNode
  count?: number
}

function SectionHeading({ eyebrow, title, description, icon, count }: SectionHeadingProps) {
  return (
    <div className="mb-6 grid grid-cols-[2.6rem_minmax(0,1fr)_auto] items-start gap-3">
      <div className="grid h-10 w-10 place-items-center rounded-xl bg-pine/8 text-pine" aria-hidden="true">
        {icon}
      </div>
      <div>
        <p className="mb-1 text-[0.63rem] font-extrabold tracking-[0.16em] text-pine/55 uppercase">{eyebrow}</p>
        <h2 className="font-display text-[1.65rem] leading-tight tracking-[-0.025em] text-ink">{title}</h2>
        <p className="mt-1 max-w-2xl text-sm leading-relaxed text-ink/55">{description}</p>
      </div>
      {count === undefined ? null : (
        <span className="rounded-full border border-pine/10 bg-white/50 px-2.5 py-1 text-xs font-bold text-pine/65">
          {count}
        </span>
      )}
    </div>
  )
}

function OutcomeBadge({ outcome }: Pick<SessionDetailDto, 'outcome'>) {
  const tone =
    outcome === 'verified_success'
      ? 'border-emerald-700/15 bg-emerald-50 text-emerald-800'
      : outcome === 'failed'
        ? 'border-red-700/15 bg-red-50 text-red-800'
        : 'border-amber-700/15 bg-amber-50 text-amber-800'
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-bold ${tone}`}>
      <CircleCheckBig aria-hidden="true" size={15} />
      {displayLabel(outcome)}
    </span>
  )
}

function DeterministicFallback({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'unavailable') {
    return null
  }
  return (
    <section
      className="grid grid-cols-[3rem_minmax(0,1fr)_auto] items-center gap-4 rounded-2xl border border-pine/15 bg-pine/[0.055] p-5 max-sm:grid-cols-[3rem_minmax(0,1fr)]"
      aria-labelledby="fallback-title"
    >
      <div className="grid h-12 w-12 place-items-center rounded-2xl bg-pine text-mint">
        <ShieldCheck aria-hidden="true" size={22} />
      </div>
      <div>
        <p className="mb-1 text-[0.63rem] font-extrabold tracking-[0.16em] text-pine/55 uppercase">Authoritative base graph</p>
        <h2 id="fallback-title" className="font-display text-xl text-ink">Deterministic view is active</h2>
        <p className="mt-1 max-w-2xl text-sm leading-relaxed text-ink/60">
          No completed enrichment is selected. This view uses verified activity kinds and statuses
          without model interpretation.
        </p>
      </div>
      <span className="rounded-full border border-pine/15 bg-white/60 px-3 py-1.5 text-[0.68rem] font-extrabold tracking-[0.04em] text-pine uppercase max-sm:col-start-2 max-sm:justify-self-start">
        {displayLabel(session.enrichment.reason)}
      </span>
    </section>
  )
}

function Provenance({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'completed') {
    return null
  }
  return (
    <section className="rounded-2xl border border-white/10 bg-ink p-6 text-paper" aria-labelledby="provenance-title">
      <div>
        <p className="mb-1 text-[0.63rem] font-extrabold tracking-[0.16em] text-mint/60 uppercase">Interpretation provenance</p>
        <h2 id="provenance-title" className="font-display text-xl">Versioned semantic overlay</h2>
      </div>
      <dl className="mt-5 grid grid-cols-4 gap-px overflow-hidden rounded-xl bg-white/10 max-md:grid-cols-2 max-sm:grid-cols-1">
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Provider</dt>
          <dd className="mt-1.5 flex items-center gap-1.5 text-sm font-bold text-mint"><Sparkles aria-hidden="true" size={15} /> Mistral</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Model</dt>
          <dd className="mt-1.5 break-all text-sm font-semibold">{session.enrichment.model}</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Prompt</dt>
          <dd className="mt-1.5 break-all text-sm font-semibold">{session.enrichment.prompt_version}</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Disclosure scope</dt>
          <dd className="mt-1.5 text-sm font-semibold">{displayLabel(session.enrichment.disclosure_scope)}</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Authorization policy digest</dt>
          <dd className="mt-1.5 break-all font-mono text-xs text-paper/75">{session.enrichment.authorization_policy_digest}</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Exact prompt digest</dt>
          <dd className="mt-1.5 break-all font-mono text-xs text-paper/75">{session.enrichment.prompt_digest}</dd>
        </div>
        <div className="bg-white/[0.045] p-4">
          <dt className="text-[0.62rem] font-bold tracking-[0.12em] text-paper/40 uppercase">Schema</dt>
          <dd className="mt-1.5 break-all text-sm font-semibold">{session.enrichment.schema_version}</dd>
        </div>
      </dl>
    </section>
  )
}

function Episodes({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'completed' || session.enrichment.episodes.length === 0) {
    return null
  }
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="episodes-title">
      <SectionHeading
        eyebrow="Narrative path"
        title="Episodes"
        description="A citation-backed reading of how the execution unfolded."
        icon={<GitBranch size={20} />}
        count={session.enrichment.episodes.length}
      />
      <div className="relative flex flex-col gap-3 before:absolute before:top-5 before:bottom-5 before:left-[1.45rem] before:w-px before:bg-pine/15">
        {session.enrichment.episodes.map((episode) => (
          <article className="relative grid grid-cols-[3rem_minmax(0,1fr)] gap-4" key={episode.episode_id}>
            <div
              className="z-10 grid h-12 w-12 place-items-center rounded-2xl border-4 border-paper bg-pine text-xs font-extrabold text-mint shadow-sm"
              aria-label={`Episode ${episode.ordinal}`}
            >
              {episode.ordinal.toString().padStart(2, '0')}
            </div>
            <div className="rounded-2xl border border-ink/8 bg-white/65 p-5">
              <h3 className="text-[0.98rem] font-extrabold tracking-[-0.01em] text-ink">{episode.title}</h3>
              <p className="mt-2 text-sm leading-relaxed text-ink/62">{episode.summary}</p>
              <div className="mt-4 flex flex-wrap items-center justify-between gap-2 border-t border-ink/7 pt-3">
                <div className="flex flex-wrap items-center gap-1.5">
                  <span className="mr-1 inline-flex items-center gap-1.5 text-[0.7rem] font-bold text-ink/45"><Activity aria-hidden="true" size={14} /> {episode.activity_ids.length} activities</span>
                  <ConfidenceBadge confidence={episode.confidence} />
                  <EpistemicBadge status={episode.epistemic_status} />
                </div>
                <CitationLinks citations={episode.citations} />
              </div>
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}

function Entities({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'completed' || session.enrichment.entities.length === 0) {
    return null
  }
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="entities-title">
      <SectionHeading
        eyebrow="Semantic index"
        title="Entities"
        description="Named concepts found in the cited execution evidence."
        icon={<Boxes size={20} />}
        count={session.enrichment.entities.length}
      />
      <ul className="grid grid-cols-2 gap-2 max-xl:grid-cols-1 max-lg:grid-cols-2 max-sm:grid-cols-1">
        {session.enrichment.entities.map((entity) => (
          <li className="flex min-w-0 items-center gap-3 rounded-xl border border-ink/7 bg-white/60 p-3" key={entity.entity_id}>
            <span className="grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-pine/8 text-pine"><Braces aria-hidden="true" size={15} /></span>
            <span className="min-w-0">
              <strong className="block truncate text-sm text-ink">{entity.name}</strong>
              <small className="mt-0.5 block text-[0.66rem] font-bold tracking-[0.05em] text-ink/40 uppercase">{displayLabel(entity.kind)}</small>
            </span>
          </li>
        ))}
      </ul>
    </section>
  )
}

function Claims({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'completed' || session.enrichment.claims.length === 0) {
    return null
  }
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="claims-title">
      <SectionHeading
        eyebrow="Evidence-backed knowledge"
        title="Claims"
        description="Non-authoritative interpretations retain confidence, status, and source anchors."
        icon={<Quote size={20} />}
        count={session.enrichment.claims.length}
      />
      <div className="grid grid-cols-2 gap-3 max-xl:grid-cols-1">
        {session.enrichment.claims.map((claim) => (
          <article className="flex flex-col rounded-2xl border border-ink/8 bg-white/65 p-5" key={claim.claim_id}>
            <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
              <VocabularyBadge value={claim.kind} />
              <CitationLinks citations={claim.citations} />
            </div>
            <h3 className="text-[0.98rem] font-extrabold tracking-[-0.01em] text-ink">{claim.title}</h3>
            <p className="mt-2 flex-1 text-sm leading-relaxed text-ink/62">{claim.statement}</p>
            <div className="mt-4 flex flex-wrap gap-1.5 border-t border-ink/7 pt-3">
              <ConfidenceBadge confidence={claim.confidence} />
              <EpistemicBadge status={claim.epistemic_status} />
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}

function Relations({ session }: SessionDetailProps) {
  if (session.enrichment.state !== 'completed' || session.enrichment.relations.length === 0) {
    return null
  }
  const names = new Map(session.enrichment.entities.map((entity) => [entity.entity_id, entity.name]))
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="relations-title">
      <SectionHeading
        eyebrow="Connected knowledge"
        title="Relations"
        description="Reified links preserve their evidence instead of becoming opaque graph edges."
        icon={<Network size={20} />}
        count={session.enrichment.relations.length}
      />
      <div className="flex flex-col gap-3">
        {session.enrichment.relations.map((relation) => (
          <article className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-4 rounded-2xl border border-ink/8 bg-white/65 p-5 max-md:grid-cols-1" key={relation.relation_id}>
            <div className="flex min-w-0 items-center gap-3 max-sm:flex-col max-sm:items-start">
              <strong className="min-w-0 truncate rounded-lg bg-pine/6 px-3 py-2 text-sm text-pine">{names.get(relation.subject_entity_id)}</strong>
              <span className="inline-flex shrink-0 items-center gap-1.5 text-[0.68rem] font-extrabold tracking-[0.05em] text-ink/45 uppercase"><ArrowRight aria-hidden="true" size={16} /> {displayLabel(relation.predicate)}</span>
              <strong className="min-w-0 truncate rounded-lg bg-pine/6 px-3 py-2 text-sm text-pine">{names.get(relation.object_entity_id)}</strong>
            </div>
            <div className="flex flex-col items-end gap-2 max-md:items-start">
              <div className="flex flex-wrap justify-end gap-1.5 max-md:justify-start">
                <ConfidenceBadge confidence={relation.confidence} />
                <EpistemicBadge status={relation.epistemic_status} />
              </div>
              <CitationLinks citations={relation.citations} />
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}

function ActivityTimeline({ session }: SessionDetailProps) {
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="activities-title">
      <SectionHeading
        eyebrow="Authoritative sequence"
        title="Deterministic activities"
        description="Verified kinds and states remain available independently of enrichment."
        icon={<Activity size={20} />}
        count={session.activities.length}
      />
      <ol className="grid grid-cols-2 gap-2 max-xl:grid-cols-1">
        {session.activities.map((activity) => (
          <li className="grid grid-cols-[2rem_minmax(0,1fr)_auto] items-center gap-3 rounded-xl border border-ink/7 bg-white/60 px-3 py-2.5" key={activity.activity_id}>
            <span className="grid h-8 w-8 place-items-center rounded-lg bg-ink/5 text-[0.68rem] font-extrabold text-ink/45">{activity.sequence}</span>
            <strong className="truncate text-sm text-ink/80">{activity.label}</strong>
            <span
              className={`rounded-full px-2 py-1 text-[0.63rem] font-extrabold tracking-[0.04em] uppercase ${
                activity.status === 'succeeded'
                  ? 'bg-emerald-100 text-emerald-800'
                  : activity.status === 'failed'
                    ? 'bg-red-100 text-red-800'
                    : activity.status === 'interrupted'
                      ? 'bg-slate-200 text-slate-700'
                      : 'bg-amber-100 text-amber-800'
              }`}
            >
              {displayLabel(activity.status)}
            </span>
          </li>
        ))}
      </ol>
    </section>
  )
}

function Sources({ session }: SessionDetailProps) {
  if (session.source_anchors.length === 0) {
    return null
  }
  return (
    <section className="rounded-3xl border border-ink/8 bg-paper/80 p-6 shadow-[0_18px_50px_rgb(8_18_15_/_0.04)] max-sm:p-4" aria-labelledby="sources-title">
      <SectionHeading
        eyebrow="Citation index"
        title="Source anchors"
        description="Content-free pointers resolve semantic assertions back to the verified archive."
        icon={<Link2 size={20} />}
        count={session.source_anchors.length}
      />
      <div className="grid grid-cols-2 gap-3 max-xl:grid-cols-1">
        {session.source_anchors.map((source) => (
          <article
            className="flex scroll-mt-24 items-start gap-3 rounded-2xl border border-ink/8 bg-white/60 p-4 outline-none transition target:border-coral target:bg-orange-50 target:shadow-[0_0_0_3px_rgb(239_118_95_/_0.16)] focus-visible:border-coral focus-visible:shadow-[0_0_0_3px_rgb(239_118_95_/_0.16)]"
            id={`source-${source.anchor_id}`}
            key={source.anchor_id}
            tabIndex={-1}
          >
            <span className="grid h-9 w-9 shrink-0 place-items-center rounded-xl bg-pine/8 text-pine"><BookOpen aria-hidden="true" size={18} /></span>
            <div>
              <h3 className="text-sm font-extrabold text-ink">{source.label}</h3>
              <p className="mt-1 text-xs font-semibold text-ink/50">{displayLabel(source.source_kind)} · record {source.record_sequence}</p>
              <span className="mt-2 block font-mono text-[0.63rem] text-ink/35">Integrity {source.content_digest.slice(0, 12)}…</span>
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}

export function SessionDetail({ session }: SessionDetailProps) {
  const isEnriched = session.enrichment.state === 'completed'
  return (
    <article className="flex min-w-0 flex-col gap-5">
      <header className="relative overflow-hidden rounded-[2rem] bg-paper p-8 shadow-[0_24px_70px_rgb(8_18_15_/_0.06)] before:absolute before:-top-24 before:-right-20 before:h-72 before:w-72 before:rounded-full before:bg-mint/35 before:blur-3xl max-sm:rounded-3xl max-sm:p-5">
        <div className="relative mb-5 inline-flex items-center gap-1.5 rounded-full border border-pine/12 bg-white/60 px-3 py-1.5 text-[0.67rem] font-extrabold tracking-[0.08em] text-pine uppercase">
          {isEnriched ? <Sparkles aria-hidden="true" size={15} /> : <ShieldCheck aria-hidden="true" size={15} />}
          {isEnriched ? 'Mistral semantic overlay' : 'Deterministic fallback'}
        </div>
        <h1 className="relative max-w-4xl font-display text-[clamp(2rem,5vw,4.25rem)] leading-[0.98] tracking-[-0.045em] text-ink">{session.display.title}</h1>
        <p className="relative mt-5 max-w-3xl text-base leading-relaxed text-ink/62 sm:text-lg">{session.display.summary}</p>
        <div className="relative mt-6 flex flex-wrap items-center gap-2">
          <OutcomeBadge outcome={session.outcome} />
          <span className="inline-flex items-center gap-1.5 rounded-full border border-ink/8 bg-white/55 px-3 py-1.5 text-xs font-bold text-ink/60"><Activity aria-hidden="true" size={15} /> {session.activities.length.toLocaleString()} activities</span>
          {session.enrichment.state === 'completed' && session.enrichment.confidence !== undefined ? (
            <ConfidenceBadge confidence={session.enrichment.confidence} />
          ) : null}
          {session.enrichment.state === 'completed' && session.enrichment.epistemic_status !== undefined ? (
            <EpistemicBadge status={session.enrichment.epistemic_status} />
          ) : null}
        </div>
      </header>

      <DeterministicFallback session={session} />
      <Provenance session={session} />
      <Episodes session={session} />
      <div className="grid grid-cols-[minmax(18rem,0.72fr)_minmax(0,1.28fr)] gap-5 max-lg:grid-cols-1">
        <Entities session={session} />
        <Claims session={session} />
      </div>
      <Relations session={session} />
      <ActivityTimeline session={session} />
      <Sources session={session} />
    </article>
  )
}
