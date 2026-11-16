import { useValues } from 'kea'
import posthog from 'posthog-js'
import { useEffect, useRef } from 'react'

import { LemonBanner } from '@posthog/lemon-ui'

import { metricsDroppedLabelsLogic } from './metricsDroppedLabelsLogic'

export function MetricsDroppedLabelsBanner(): JSX.Element | null {
    const { droppedLabels, metricName } = useValues(metricsDroppedLabelsLogic)

    // Count one view per metric. The keys are the customer's own label names, so only the count goes out.
    const reportedMetric = useRef<string | null>(null)
    useEffect(() => {
        if (droppedLabels.length > 0 && reportedMetric.current !== metricName) {
            reportedMetric.current = metricName
            posthog.capture('metrics dropped labels banner shown', { dropped_label_count: droppedLabels.length })
        }
    }, [droppedLabels, metricName])

    if (droppedLabels.length === 0) {
        return null
    }

    return (
        <LemonBanner type="info">
            <div>
                PostHog removed {droppedLabels.length === 1 ? 'a label' : 'labels'} from this metric because{' '}
                {droppedLabels.length === 1 ? 'it has' : 'they have'} too many distinct values:{' '}
                {droppedLabels.map((key, index) => (
                    <span key={key}>
                        {index > 0 && ', '}
                        <code>{key}</code>
                    </span>
                ))}
                . Series that differed only by {droppedLabels.length === 1 ? 'this label' : 'these labels'} are combined
                into one, so counter rates and gauge values for those series can be wrong. Reduce the number of values
                the sender sets to keep the series apart.
            </div>
        </LemonBanner>
    )
}
