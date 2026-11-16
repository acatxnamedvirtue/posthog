// pinned: capture-apm-metrics writes this attribute on every series it combined after dropping a
// high cardinality label. Renaming it here hides the marker until capture changes too.
export const DROPPED_LABELS_ATTRIBUTE = 'posthog.dropped_labels'

export function droppedLabelKeys(markerValues: string[]): string[] {
    const keys = new Set(
        markerValues.flatMap((value) =>
            value
                .split(',')
                .map((key) => key.trim())
                .filter(Boolean)
        )
    )
    return Array.from(keys).sort()
}
