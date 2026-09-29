import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as CodexClient from "effect-codex-app-server/client";
import type * as CodexSchema from "effect-codex-app-server/schema";

const MCP_STARTUP_TIMEOUT = "30 seconds";
type StartupUpdate = CodexSchema.V2McpServerStatusUpdatedNotification;

/** Reload acknowledges queued work, not a complete model-facing tool catalog. */
export const makeCodexMcpStartup = Effect.fnUntraced(function* () {
  const client = yield* CodexClient.CodexAppServerClient;
  const semaphore = yield* Semaphore.make(1);
  let active:
    | {
        threadId: string;
        statuses: Map<string, StartupUpdate["status"]>;
        started: Set<string>;
        changed: Queue.Queue<void>;
      }
    | undefined;

  yield* client.handleServerNotification("mcpServer/startupStatus/updated", (update) =>
    Effect.gen(function* () {
      // Unscoped notifications cannot prove readiness for this thread's reload.
      if (!active || update.threadId !== active.threadId) return;
      if (update.status === "starting") active.started.add(update.name);
      else if (update.status !== "ready" && !active.started.has(update.name)) return;
      active.statuses.set(update.name, update.status);
      yield* Queue.offer(active.changed, undefined);
    }),
  );

  return Effect.fnUntraced(function* (threadId: string) {
    const changed = yield* Queue.sliding<void>(1);
    const statuses = new Map<string, StartupUpdate["status"]>();
    const started = new Set<string>();
    const connected = new Set<string>();
    active = { threadId, statuses, changed, started };
    const expected = new Set<string>();
    const isSettled = (name: string) => {
      const status = statuses.get(name);
      // Codex can emit cancelled before ready during a reload. Keep waiting
      // for ready/failed; a genuinely cancelled server is bounded by the timeout.
      // Reused connections report ready without starting again. Require the
      // inventory to agree; neither signal identifies the reload generation.
      return (
        (started.has(name) && (status === "ready" || status === "failed")) ||
        (status === "ready" && connected.has(name))
      );
    };

    yield* Effect.gen(function* () {
      yield* client.request("config/mcpServer/reload", undefined);
      let cursor: string | undefined;
      const cursors = new Set<string>();
      do {
        const page = yield* client.request("mcpServerStatus/list", {
          threadId,
          detail: "toolsAndAuthOnly",
          ...(cursor ? { cursor } : {}),
        });
        for (const server of page.data) {
          if (server.runtimeStatus !== "disabled") expected.add(server.name);
          if (server.runtimeStatus === "connected") connected.add(server.name);
        }
        cursor = page.nextCursor ?? undefined;
        if (cursor && cursors.has(cursor)) {
          yield* Effect.logWarning("Codex MCP status pagination repeated a cursor.");
          return;
        }
        if (cursor) cursors.add(cursor);
      } while (cursor);

      // Only notifications observed during this wait can settle a server. A snapshot may
      // still describe the old connections while the queued reload is starting.
      while ([...new Set([...expected, ...statuses.keys()])].some((name) => !isSettled(name))) {
        yield* Queue.take(changed);
      }
    }).pipe(
      Effect.timeoutOption(MCP_STARTUP_TIMEOUT),
      Effect.flatMap((result) =>
        Option.isNone(result)
          ? Effect.logWarning("Timed out waiting for Codex MCP startup; tools may be incomplete.", {
              threadId,
              pendingServerCount: [...new Set([...expected, ...statuses.keys()])].filter(
                (name) => !isSettled(name),
              ).length,
            })
          : Effect.void,
      ),
      Effect.catch((cause) =>
        Effect.logWarning("Failed to refresh Codex MCP tool catalog before turn.", { cause }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          active = undefined;
        }).pipe(Effect.andThen(Queue.shutdown(changed))),
      ),
    );
  }, semaphore.withPermits(1));
});
