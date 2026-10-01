import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as CodexClient from "effect-codex-app-server/client";
import type * as CodexSchema from "effect-codex-app-server/schema";

const MCP_STARTUP_TIMEOUT = "30 seconds";
// The reload reply only queues the refresh. Codex then sends `starting` for every
// restarting server within milliseconds, so wait for the first update, then until
// no update arrives for BURST_QUIET. Terminal updates can arrive later.
const FIRST_UPDATE_GRACE = "500 millis";
const BURST_QUIET = "100 millis";
type StartupStatus = CodexSchema.V2McpServerStatusUpdatedNotification["status"];

/**
 * Returns a per-turn refresh: reload the thread's MCP servers, then wait until none
 * is still starting. Reload acknowledges queued work, not a complete tool catalog.
 */
export const makeCodexMcpStartup = Effect.fnUntraced(function* () {
  const client = yield* CodexClient.CodexAppServerClient;
  const semaphore = yield* Semaphore.make(1);
  const changed = yield* Queue.sliding<void>(1);
  // Latest status per thread and server for the whole session, so the `starting`
  // sent when a thread opens is not lost before its first turn.
  const statuses = new Map<string, Map<string, StartupStatus>>();
  const updateCounts = new Map<string, number>();
  const statusesFor = (threadId: string) => {
    let servers = statuses.get(threadId);
    if (!servers) statuses.set(threadId, (servers = new Map()));
    return servers;
  };

  yield* client.handleServerNotification("mcpServer/startupStatus/updated", (update) =>
    Effect.gen(function* () {
      // Unscoped notifications cannot be attributed to a thread.
      if (!update.threadId) return;
      updateCounts.set(update.threadId, (updateCounts.get(update.threadId) ?? 0) + 1);
      const servers = statusesFor(update.threadId);
      // Codex can emit cancelled before ready during a reload; a later ready wins.
      // A failed server restarts on every reload. Wait for it again only after it recovers.
      const restartOfFailed = update.status === "starting" && servers.get(update.name) === "failed";
      if (update.status !== "cancelled" && !restartOfFailed)
        servers.set(update.name, update.status);
      // Wake the waiter last, so it never reads the map before this update is in it.
      yield* Queue.offer(changed, undefined);
    }),
  );

  return Effect.fnUntraced(function* (threadId: string) {
    const servers = statusesFor(threadId);
    const updatesBeforeReload = updateCounts.get(threadId) ?? 0;
    const startingServers = () =>
      [...servers].filter(([, status]) => status === "starting").map(([name]) => name);

    yield* Effect.gen(function* () {
      yield* client.request("config/mcpServer/reload", undefined);
      yield* Effect.gen(function* () {
        while ((updateCounts.get(threadId) ?? 0) === updatesBeforeReload) {
          yield* Queue.take(changed);
        }
      }).pipe(Effect.timeoutOption(FIRST_UPDATE_GRACE));
      // Deciding on the first update alone misses a `starting` later in the same burst.
      let burstEnded = false;
      while (!burstEnded) {
        burstEnded = Option.isNone(
          yield* Queue.take(changed).pipe(Effect.timeoutOption(BURST_QUIET)),
        );
      }
      while (startingServers().length > 0) yield* Queue.take(changed);
    }).pipe(
      Effect.timeoutOption(MCP_STARTUP_TIMEOUT),
      Effect.flatMap((result) => {
        if (Option.isSome(result)) return Effect.void;
        const pending = startingServers();
        // A server that outlasts the wait counts as failed, so later turns do not wait for it again.
        for (const name of pending) servers.set(name, "failed");
        return Effect.logWarning(
          "Timed out waiting for Codex MCP startup; tools may be incomplete.",
          {
            threadId,
            pendingServerCount: pending.length,
          },
        );
      }),
      Effect.catch((cause) =>
        Effect.logWarning("Failed to refresh Codex MCP tool catalog before turn.", { cause }),
      ),
    );
  }, semaphore.withPermits(1));
});
