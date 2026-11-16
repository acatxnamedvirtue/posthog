import { Meta, StoryObj } from '@storybook/react'
import { useMountedLogic } from 'kea'
import { useEffect } from 'react'

import { mswDecorator } from '~/mocks/browser'

import { metricsViewerLogic } from '../metricsViewerLogic'
import { MetricsDroppedLabelsBanner } from './MetricsDroppedLabelsBanner'

const meta: Meta<typeof MetricsDroppedLabelsBanner> = {
    title: 'Metrics/Dropped labels banner',
    component: MetricsDroppedLabelsBanner,
    decorators: [
        mswDecorator({
            get: {
                '/api/projects/:team_id/metrics/attribute_values/': {
                    results: [
                        { id: 'user_id,request_id', name: 'user_id,request_id', count: 40 },
                        { id: 'user_id', name: 'user_id', count: 12 },
                    ],
                },
            },
        }),
    ],
    parameters: { layout: 'padded', testOptions: { snapshotBrowsers: [] } },
    render: function Render() {
        const viewer = useMountedLogic(metricsViewerLogic)
        useEffect(() => {
            viewer.actions.setMetricName('http_server_request_duration')
        }, [viewer])
        return <MetricsDroppedLabelsBanner />
    },
}

export default meta
type Story = StoryObj<typeof MetricsDroppedLabelsBanner>
export const Default: Story = {}
