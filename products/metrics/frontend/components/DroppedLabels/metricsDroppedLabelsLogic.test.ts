import { expectLogic } from 'kea-test-utils'

import { initKeaTests } from '~/test/init'
import { AccessControlLevel, AccessControlResourceType, AppContext } from '~/types'

import { metricsAttributeValuesRetrieve } from '../../generated/api'
import { metricsViewerLogic } from '../metricsViewerLogic'
import { DROPPED_LABELS_ATTRIBUTE } from './droppedLabels'
import { metricsDroppedLabelsLogic } from './metricsDroppedLabelsLogic'

jest.mock('../../generated/api', () => ({
    ...jest.requireActual('../../generated/api'),
    metricsAttributeValuesRetrieve: jest.fn(),
}))

describe('metricsDroppedLabelsLogic', () => {
    let viewer: ReturnType<typeof metricsViewerLogic.build>
    let logic: ReturnType<typeof metricsDroppedLabelsLogic.build>

    beforeEach(() => {
        window.POSTHOG_APP_CONTEXT = {
            ...window.POSTHOG_APP_CONTEXT,
            resource_access_control: {
                ...window.POSTHOG_APP_CONTEXT?.resource_access_control,
                [AccessControlResourceType.Metrics]: AccessControlLevel.Viewer,
            },
        } as AppContext
        initKeaTests()
        jest.mocked(metricsAttributeValuesRetrieve)
            .mockReset()
            .mockResolvedValue({
                results: [
                    { id: 'user_id,request_id', name: 'user_id,request_id', count: 40 },
                    { id: 'user_id', name: 'user_id', count: 12 },
                ],
            })
        viewer = metricsViewerLogic()
        viewer.mount()
        logic = metricsDroppedLabelsLogic()
        logic.mount()
    })

    // The mount loads once for the empty metric name, so wait for the load of the metric under test.
    const loadedFor = (metricName: string) => (action: { type: string; payload?: any }) =>
        action.type === logic.actionTypes.loadDroppedLabelsSuccess &&
        JSON.parse(action.payload.droppedLabelsResult.scope)[0] === metricName

    afterEach(() => {
        logic.unmount()
        viewer.unmount()
    })

    it('lists the distinct keys of the marker values of the current metric', async () => {
        viewer.actions.setMetricName('http_requests')

        await expectLogic(logic).toDispatchActions([loadedFor('http_requests')])

        expect(jest.mocked(metricsAttributeValuesRetrieve).mock.lastCall?.[1]).toMatchObject({
            key: DROPPED_LABELS_ATTRIBUTE,
            metricName: 'http_requests',
        })
        expect(logic.values.droppedLabels).toEqual(['request_id', 'user_id'])
    })

    it('hides the keys of the previous metric until the new metric answers', async () => {
        viewer.actions.setMetricName('http_requests')
        await expectLogic(logic).toDispatchActions([loadedFor('http_requests')])
        expect(logic.values.droppedLabels).not.toEqual([])

        viewer.actions.setMetricName('queue_depth')

        expect(logic.values.droppedLabels).toEqual([])
    })
})
