"""
Service Page Module

Contains the inline HTML/JS for the headless 3P service that runs
inside the Amazon Connect Agent Workspace as a hidden iframe.

The SDK is loaded from a pre-built esbuild bundle that exposes
ConnectSDK.AmazonConnectService and ConnectSDK.AgentClient on window.

The application JavaScript uses ES2020+ features:
  - Private class fields (#)
  - Optional chaining (?.)
  - Nullish coalescing (??)
  - crypto.randomUUID()
  - AbortController for fetch timeout
  - Static class methods
  - Array.prototype.at()
"""


def build_service_html(sdk_bundle_js):
    """
    Build the complete service HTML with the SDK bundle inlined.

    Args:
        sdk_bundle_js: The contents of the esbuild SDK bundle (string).

    Returns:
        Complete HTML string ready to serve.
    """
    return (
        SERVICE_HTML_HEAD
        + sdk_bundle_js
        + SERVICE_HTML_TAIL
    )


# ──────────────────────────────────────────────
# HTML TEMPLATE (split around SDK injection point)
# ──────────────────────────────────────────────

SERVICE_HTML_HEAD = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Agent State Tracker Service</title>
</head>
<body>
  <!-- Headless service: no visible UI elements -->

  <!-- Amazon Connect SDK bundle (esbuild IIFE) -->
  <script>
"""

SERVICE_HTML_TAIL = r"""
  </script>

  <!-- Agent State Tracker service logic -->
  <script>
    const { AmazonConnectService, AgentClient } = window.ConnectSDK;

    // ──────────────────────────────────────────
    // LOGGER
    // ──────────────────────────────────────────
    class Logger {
      static #PREFIX = '[AgentStateTracker]';

      static #format(level, msg) {
        return `${Logger.#PREFIX} ${new Date().toISOString()} [${level}] ${msg}`;
      }

      static info(msg)  { console.info(Logger.#format('INFO', msg)); }
      static warn(msg)  { console.warn(Logger.#format('WARN', msg)); }
      static error(msg) { console.error(Logger.#format('ERROR', msg)); }
      static debug(msg) { console.debug(Logger.#format('DEBUG', msg)); }
    }

    // ──────────────────────────────────────────
    // EVENT BUFFER
    // ──────────────────────────────────────────
    class EventBuffer {
      #buffer = [];

      get length() { return this.#buffer.length; }

      push(event) {
        this.#buffer.push(Object.freeze(event));
      }

      drain() {
        const items = [...this.#buffer];
        this.#buffer.length = 0;
        return items;
      }

      prepend(events) {
        this.#buffer.unshift(...events);
      }
    }

    // ──────────────────────────────────────────
    // NETWORK CLIENT
    // ──────────────────────────────────────────
    class NetworkClient {
      #endpointUrl;
      #fetchTimeoutMs;

      constructor(endpointUrl, { fetchTimeoutMs = 10_000 } = {}) {
        this.#endpointUrl = endpointUrl;
        this.#fetchTimeoutMs = fetchTimeoutMs;
      }

      async send(payload) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.#fetchTimeoutMs);

        try {
          const response = await fetch(this.#endpointUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: controller.signal,
            keepalive: true,
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status} ${response.statusText}`);
          }

          return await response.json();
        } finally {
          clearTimeout(timeoutId);
        }
      }

      sendBeacon(payload) {
        const blob = new Blob(
          [JSON.stringify(payload)],
          { type: 'application/json' },
        );
        const sent = navigator.sendBeacon(this.#endpointUrl, blob);
        Logger.info(`sendBeacon dispatched: ${sent ? 'success' : 'failed'}`);
        return sent;
      }
    }

    // ──────────────────────────────────────────
    // RETRY HANDLER
    // ──────────────────────────────────────────
    class RetryHandler {
      #maxRetries;
      #currentAttempt = 0;
      #maxBackoffMs;

      constructor({ maxRetries = 3, maxBackoffMs = 30_000 } = {}) {
        this.#maxRetries = maxRetries;
        this.#maxBackoffMs = maxBackoffMs;
      }

      get canRetry() {
        return this.#currentAttempt <= this.#maxRetries;
      }

      get backoffMs() {
        const base = 1_000 * (2 ** this.#currentAttempt);
        const jitter = Math.random() * 500;
        return Math.min(base + jitter, this.#maxBackoffMs);
      }

      increment() { this.#currentAttempt++; }
      reset()     { this.#currentAttempt = 0; }
    }

    // ──────────────────────────────────────────
    // AGENT STATE TRACKER (main service)
    // ──────────────────────────────────────────
    class AgentStateTracker {
      static #FLUSH_INTERVAL_MS     = 5_000;
      static #HEARTBEAT_INTERVAL_MS = 60_000;

      #agentArn    = '';
      #agentName   = '';
      #sessionId   = crypto.randomUUID();
      #lastStatus  = 'Unknown';
      #lastTransitionTs = null;

      #buffer      = new EventBuffer();
      #network;
      #retry       = new RetryHandler();
      #flushTimer  = null;
      #heartbeatTimer = null;

      constructor(endpointUrl) {
        this.#network = new NetworkClient(endpointUrl);
      }

      // ── Public API ──────────────────────────

      async initialize(provider) {
        const agentClient = new AgentClient({ provider });
        await this.#identifyAgent(agentClient);
        await this.#captureInitialState(agentClient);
        this.#subscribeToStateChanges(agentClient);
        this.#startFlushTimer();
        this.#startHeartbeat();
        this.#registerUnloadHandler();
        Logger.info('Agent tracking initialized successfully');
      }

      // ── Agent Identification ────────────────

      async #identifyAgent(agentClient) {
        try {
          const arn = await agentClient.getARN();
          this.#agentArn = arn;
        } catch (err) {
          Logger.warn(`Could not retrieve agent ARN: ${err.message}`);
        }

        try {
          const name = await agentClient.getName();
          this.#agentName = name ?? this.#agentArn.split('/').at(-1) ?? 'Unknown';
        } catch (err) {
          Logger.warn(`Could not retrieve agent name: ${err.message}`);
          this.#agentName = this.#agentArn.split('/').at(-1) ?? 'Unknown';
        }

        Logger.info(`Agent identified: ${this.#agentName} (${this.#agentArn})`);
      }

      async #captureInitialState(agentClient) {
        try {
          const currentState = await agentClient.getState();
          this.#lastStatus = currentState?.name ?? 'Unknown';
          this.#lastTransitionTs = currentState?.startTimestamp
            ? new Date(currentState.startTimestamp).toISOString()
            : new Date().toISOString();
          Logger.info(`Initial state: ${this.#lastStatus}`);
        } catch (err) {
          Logger.warn(`Could not capture initial state: ${err.message}`);
          this.#lastTransitionTs = new Date().toISOString();
        }
      }

      // ── State Change Subscription ───────────

      #subscribeToStateChanges(agentClient) {
        agentClient.onStateChanged((data) => {
          const now = new Date().toISOString();

          const event = {
            new_status:               data?.state ?? 'Unknown',
            previous_status:          data?.previous?.state ?? this.#lastStatus ?? 'Unknown',
            timestamp:                now,
            previous_start_timestamp: this.#lastTransitionTs ?? now,
          };

          this.#lastStatus = event.new_status;
          this.#lastTransitionTs = now;
          this.#buffer.push(event);

          Logger.info(
            `Transition: ${event.previous_status} → ${event.new_status} ` +
            `(buffered: ${this.#buffer.length})`
          );
        });
      }

      // ── Batch Flush ─────────────────────────

      #startFlushTimer() {
        this.#flushTimer = setInterval(
          () => this.#flush(),
          AgentStateTracker.#FLUSH_INTERVAL_MS,
        );
      }

      async #flush() {
        if (this.#buffer.length === 0) return;

        const batch = this.#buffer.drain();
        const payload = this.#buildPayload(batch, 'state_change');

        try {
          const result = await this.#network.send(payload);
          this.#retry.reset();
          Logger.info(
            `Flushed ${batch.length} events ` +
            `(processed: ${result?.processed ?? '?'})`
          );
        } catch (err) {
          Logger.warn(
            `Flush failed: ${err.message} — re-queuing ${batch.length} events`
          );
          this.#buffer.prepend(batch);
          this.#retry.increment();

          if (this.#retry.canRetry) {
            const delay = this.#retry.backoffMs;
            Logger.info(`Retrying in ${Math.round(delay)}ms`);
            setTimeout(() => this.#flush(), delay);
          } else {
            Logger.error(`Max retries exceeded — dropping ${batch.length} events`);
            this.#buffer.drain();
            this.#retry.reset();
          }
        }
      }

      // ── Heartbeat ───────────────────────────

      #startHeartbeat() {
        this.#heartbeatTimer = setInterval(async () => {
          try {
            await this.#network.send(this.#buildPayload([], 'heartbeat'));
            Logger.debug('Heartbeat sent');
          } catch (err) {
            Logger.warn(`Heartbeat failed: ${err.message}`);
          }
        }, AgentStateTracker.#HEARTBEAT_INTERVAL_MS);
      }

      // ── Unload Safety Net ───────────────────

      #registerUnloadHandler() {
        globalThis.addEventListener('beforeunload', () => {
          clearInterval(this.#flushTimer);
          clearInterval(this.#heartbeatTimer);

          if (this.#buffer.length > 0) {
            const batch = this.#buffer.drain();
            this.#network.sendBeacon(this.#buildPayload(batch, 'state_change_final'));
            Logger.info(`Beacon sent with ${batch.length} remaining events`);
          }
        });
      }

      // ── Payload Builder ─────────────────────

      #buildPayload(events, type) {
        return {
          events,
          type,
          session_id: this.#sessionId,
          agent_arn:  this.#agentArn,
          agent_name: this.#agentName,
        };
      }
    }

    // ──────────────────────────────────────────
    // SERVICE ENTRY POINT
    // ──────────────────────────────────────────
    const endpointUrl = `${globalThis.location.origin}${globalThis.location.pathname}`;
    const tracker = new AgentStateTracker(endpointUrl);

    AmazonConnectService.init({
      onCreate: async (event) => {
        const instanceId = event?.context?.instanceId ?? 'unknown';
        Logger.info(`Service created for Connect instance: ${instanceId}`);

        try {
          const provider = event.context.getProvider();
          await tracker.initialize(provider);
        } catch (err) {
          Logger.error(`Failed to initialize tracker: ${err.message}`);
        }
      },
    });
  </script>
</body>
</html>"""
