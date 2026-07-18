import { BadgeCheck, BookOpenCheck, CircleDashed, Sparkles } from 'lucide-react'
import type { Confidence, EpistemicStatus } from '../api/contracts'

function titleCase(value: string): string {
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

interface ConfidenceBadgeProps {
  confidence: Confidence
}

export function ConfidenceBadge({ confidence }: ConfidenceBadgeProps) {
  const Icon = confidence === 'high' ? BadgeCheck : confidence === 'medium' ? CircleDashed : Sparkles
  const tone =
    confidence === 'high'
      ? 'border-emerald-700/20 bg-emerald-50 text-emerald-800'
      : confidence === 'medium'
        ? 'border-amber-700/20 bg-amber-50 text-amber-800'
        : 'border-slate-500/20 bg-slate-100 text-slate-700'
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[0.68rem] font-bold tracking-[0.02em] ${tone}`}
      title="Model confidence"
    >
      <Icon aria-hidden="true" size={13} strokeWidth={2.2} />
      {titleCase(confidence)} confidence
    </span>
  )
}

interface EpistemicBadgeProps {
  status: EpistemicStatus
}

export function EpistemicBadge({ status }: EpistemicBadgeProps) {
  const Icon = status === 'explicit' ? BookOpenCheck : status === 'inferred' ? Sparkles : CircleDashed
  const tone =
    status === 'explicit'
      ? 'border-teal-700/20 bg-teal-50 text-teal-800'
      : status === 'inferred'
        ? 'border-violet-700/20 bg-violet-50 text-violet-800'
        : 'border-amber-700/20 bg-amber-50 text-amber-800'
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[0.68rem] font-bold tracking-[0.02em] ${tone}`}
      title="Evidence status"
    >
      <Icon aria-hidden="true" size={13} strokeWidth={2.2} />
      {titleCase(status)}
    </span>
  )
}

interface VocabularyBadgeProps {
  value: string
}

export function VocabularyBadge({ value }: VocabularyBadgeProps) {
  return (
    <span className="inline-flex items-center rounded-full border border-pine/15 bg-pine/5 px-2.5 py-1 text-[0.66rem] font-extrabold tracking-[0.08em] text-pine uppercase">
      {titleCase(value)}
    </span>
  )
}
