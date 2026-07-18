import { ArrowDownToLine } from 'lucide-react'

interface Citation {
  anchor_id: string
}

interface CitationLinksProps {
  citations: readonly Citation[]
}

export function CitationLinks({ citations }: CitationLinksProps) {
  if (citations.length === 0) {
    return null
  }

  return (
    <div className="flex flex-wrap items-center gap-1.5" aria-label="Source citations">
      {citations.map((citation, index) => (
        <a
          className="inline-flex items-center gap-1 rounded-full border border-pine/15 bg-white/70 px-2 py-1 text-[0.68rem] font-bold text-pine transition hover:border-pine/30 hover:bg-white focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-pine"
          href={`#source-${citation.anchor_id}`}
          key={citation.anchor_id}
          aria-label={`Go to source citation ${index + 1}`}
        >
          <ArrowDownToLine aria-hidden="true" size={13} />
          Evidence {index + 1}
        </a>
      ))}
    </div>
  )
}
