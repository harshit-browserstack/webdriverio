import { spawn } from 'node:child_process'
import path from 'node:path'
import BrowserStackConfig from './config.js'
import { saveFunnelData } from './instrumentation/funnelInstrumentation.js'
import { fileURLToPath } from 'node:url'
import { BROWSERSTACK_TESTHUB_JWT } from './constants.js'
import PerformanceTester from './instrumentation/performance/performance-tester.js'
import TestOpsConfig from './testOps/testOpsConfig.js'
import { BStackLogger } from './bstackLogger.js'
import { BrowserstackCLI } from './cli/index.js'
import { stopBuildUpstream } from './util.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

// SDK-6050: bounded grace period for async signal-driven build-stop cleanup
const SIGNAL_CLEANUP_GRACE_MS = 10_000

// Idempotency guard — once cleanup starts, ignore subsequent signals (R5/R6).
// Module-scope so multiple service instances / multiple signals converge here.
let signalCleanupInFlight = false
let signalHandlersInstalled = false

/**
 * SDK-6050: async signal handler that mirrors the graceful `onComplete()` build-stop
 * (`launcher.ts:586-590`) so killing the WDIO process mid-run still emits a stop event
 * to TestHub. Without this, only the synchronous `process.on('exit')` runs — which
 * cannot await `BrowserstackCLI.stop()` or `stopBuildUpstream()`.
 *
 * Behavior:
 *  - SIGINT, SIGTERM, SIGHUP, SIGABRT, SIGQUIT (+ SIGBREAK on Windows).
 *  - First signal triggers cleanup; subsequent signals are ignored.
 *  - Cleanup runs with a {@link SIGNAL_CLEANUP_GRACE_MS} timeout — process exits anyway after.
 *  - After cleanup (or timeout), exit code 128+signum so callers can distinguish.
 */
function setupSignalHandlers() {
    if (signalHandlersInstalled) {
        return
    }
    signalHandlersInstalled = true

    const signals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM', 'SIGHUP', 'SIGABRT', 'SIGQUIT']
    if (process.platform === 'win32') {
        signals.push('SIGBREAK')
    }

    // Signal-number mapping for exit-code (POSIX convention: 128 + signum).
    const signalToNum: Record<string, number> = {
        SIGINT: 2,
        SIGTERM: 15,
        SIGHUP: 1,
        SIGABRT: 6,
        SIGQUIT: 3,
        SIGBREAK: 21,
    }

    const handler = async (signal: NodeJS.Signals) => {
        if (signalCleanupInFlight) {
            BStackLogger.debug(`Signal ${signal} received again; cleanup already in flight, ignoring`)
            return
        }
        signalCleanupInFlight = true

        // Defense-in-depth: re-prune any listeners that may have been added
        // after install. Note that Node snapshots the listener array at the
        // start of emit() — removals here do NOT prevent already-snapshotted
        // listeners from firing in THIS emit cycle. That's why the primary
        // prune happens at install time (see setupSignalHandlers below).
        try {
            const listeners = process.listeners(signal).slice()
            for (const l of listeners) {
                if (l !== currentHandlerWrappers.get(signal)) {
                    process.removeListener(signal, l as (...args: unknown[]) => void)
                }
            }
        } catch {
            // best-effort; never let listener-manipulation errors block cleanup
        }

        BStackLogger.debug(`Signal ${signal} received; starting async build-stop cleanup (grace ${SIGNAL_CLEANUP_GRACE_MS}ms)`)

        const exitCode = 128 + (signalToNum[signal] ?? 0)

        const cleanup = (async () => {
            try {
                const isCLIEnabled = BrowserstackCLI.getInstance().isRunning()
                BStackLogger.debug(`Sending stop launch event (signal=${signal}, mode=${isCLIEnabled ? 'cli' : 'direct'})`)
                await (isCLIEnabled ? BrowserstackCLI.getInstance().stop(signal) : stopBuildUpstream(signal))
                BrowserStackConfig.getInstance().testObservability.buildStopped = true
                BStackLogger.debug(`Build-stop completed for signal ${signal}`)
            } catch (err) {
                BStackLogger.debug(`Build-stop on signal ${signal} failed: ${err}`)
            }
        })()

        const timeout = new Promise<void>((resolve) => setTimeout(() => {
            BStackLogger.debug(`Signal cleanup grace period (${SIGNAL_CLEANUP_GRACE_MS}ms) elapsed for ${signal}; exiting`)
            resolve()
        }, SIGNAL_CLEANUP_GRACE_MS))

        await Promise.race([cleanup, timeout])

        // After cleanup (or timeout), exit so Node doesn't continue indefinitely.
        process.exit(exitCode)
    }

    // Track which wrapper we registered per-signal, so the listener-pruning
    // above can identify and preserve ours while removing competing handlers.
    const currentHandlerWrappers = new Map<NodeJS.Signals, (...args: unknown[]) => void>()

    for (const sig of signals) {
        const wrapper = () => {
            // Fire-and-forget; node ignores the returned promise on signal handlers anyway.
            handler(sig).catch((err) => {
                BStackLogger.debug(`Unhandled error in signal cleanup for ${sig}: ${err}`)
                process.exit(128 + (signalToNum[sig] ?? 0))
            })
        }
        currentHandlerWrappers.set(sig, wrapper)

        // Critical: snapshot AND remove existing listeners for THIS signal at
        // install time. Once an emit() begins, Node iterates a frozen snapshot
        // of the listener array — removals during emit have NO effect on the
        // current emit cycle. Competing handlers (e.g. create-wdio's top-level
        // `process.on('SIGINT', () => process.exit(1))` at
        // packages/create-wdio/src/utils.ts:29, and `async-exit-hook`'s SIGINT
        // hook registered transitively by @wdio/cli) call `process.exit()`
        // synchronously, killing our async cleanup mid-await.
        //
        // setupExitHandlers() is invoked from the BrowserstackLauncherService
        // constructor (launcher.ts:88), which runs AFTER @wdio/cli has loaded
        // create-wdio and async-exit-hook — so by this point their handlers
        // are already registered and we can prune them cleanly.
        try {
            const existing = process.listeners(sig).slice()
            for (const l of existing) {
                process.removeListener(sig, l as (...args: unknown[]) => void)
            }
            if (existing.length > 0) {
                BStackLogger.debug(`Removed ${existing.length} pre-existing ${sig} listener(s) at install time`)
            }
        } catch {
            // best-effort; never let listener-manipulation errors block install
        }

        // Now register ours. prependListener (rather than on) is belt-and-braces
        // in case a later library still manages to slip a listener in front.
        process.prependListener(sig, wrapper)
    }
}

export function setupExitHandlers() {
    setupSignalHandlers()
    const handleCLICleanup = () => {
        BStackLogger.debug('Handling CLI cleanup in exit handler')
        try {
            const cliProcess = BrowserstackCLI.getInstance()?.process

            if (cliProcess && cliProcess.pid && cliProcess.exitCode === null) {
                BStackLogger.debug(`Found CLI process with PID ${cliProcess.pid}, terminating`)
                try {
                    if (process.platform === 'win32') {
                        cliProcess.kill('SIGTERM')
                        BStackLogger.debug('CLI process terminated successfully with SIGTERM (Windows)')
                    } else {
                        cliProcess.kill('SIGINT')
                        BStackLogger.debug('CLI process terminated successfully with SIGINT (Unix)')
                    }
                } catch (processError) {
                    BStackLogger.debug(`CLI process termination error: ${processError}`)
                    try {
                        cliProcess.kill()
                        BStackLogger.debug('CLI process terminated with default signal (fallback)')
                    } catch (fallbackError) {
                        BStackLogger.debug(`CLI process fallback termination error: ${fallbackError}`)
                    }
                }
            } else {
                BStackLogger.debug('No CLI process found to terminate')
            }
        } catch (error) {
            BStackLogger.debug(`Error in CLI cleanup: ${error}`)
        }
    }
    process.on('exit', () => {
        const isCLIEnabled = BrowserstackCLI.getInstance().isRunning()
        handleCLICleanup()
        const args = shouldCallCleanup(BrowserStackConfig.getInstance(), isCLIEnabled)
        if (Array.isArray(args) && args.length) {
            BStackLogger.debug(`Spawning cleanup.js with args: ${args.join(', ')}`)
            const childProcess = spawn('node', [`${path.join(__dirname, 'cleanup.js')}`, ...args], { detached: true, stdio: 'inherit', env: { ...process.env } })
            childProcess.unref()
        }
    })
}

export function shouldCallCleanup(config: BrowserStackConfig, isCLIEnabled = false): string[] {
    const args: string[] = []
    if (!!process.env[BROWSERSTACK_TESTHUB_JWT] && !config.testObservability.buildStopped) {
        args.push('--observability')
    }

    if (config.userName && config.accessKey && !config.funnelDataSent) {
        const savedFilePath = saveFunnelData('SDKTestSuccessful', config, isCLIEnabled)
        args.push('--funnelData', savedFilePath)
    }

    if (PerformanceTester.isEnabled()) {
        process.env.PERF_USER_NAME = config.userName
        process.env.PERF_TESTHUB_UUID = TestOpsConfig.getInstance().buildHashedId
        process.env.SDK_RUN_ID = config.sdkRunID
        args.push('--performanceData')
    }

    return args
}
