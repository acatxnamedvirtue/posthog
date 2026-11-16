import type { PostHog } from 'posthog-js/dist/module'

// App code shared with the toolbar reads the mounted toolbar from here. Importing toolbarConfigLogic instead would ship
// the toolbar's session code on every page of the app, so this module must not import any toolbar module.

export interface ToolbarRuntime {
    uiHost: () => string
    posthog: () => PostHog | null
    isAuthenticated: () => boolean
    authenticate: () => void
}

let mountedRuntime: ToolbarRuntime | null = null

/** Registers the mounted toolbar and returns the function that unregisters it. */
export function registerToolbarRuntime(runtime: ToolbarRuntime): () => void {
    mountedRuntime = runtime
    return () => {
        if (mountedRuntime === runtime) {
            mountedRuntime = null
        }
    }
}

/** The mounted toolbar, or null when the code runs in the PostHog app. */
export function getToolbarRuntime(): ToolbarRuntime | null {
    return mountedRuntime
}
