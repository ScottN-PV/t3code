import {
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ThreadShell,
  OrchestratorMcpFailure,
  type OrchestratorMcpThreadSettleInput,
  type OrchestratorMcpThreadSettleResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as McpInvocationContext from "./McpInvocationContext.ts";
import { dispatchFailure, newCommandId, readThread, unavailable } from "./threadAccess.ts";

/**
 * Threads an agent started through T3's own MCP tools (`t3_thread_launch`,
 * `create_threads`, `delegate_task`). Threads the user started are never
 * settled by an agent.
 */
const isAgentLaunched = (thread: Pick<OrchestrationV2AppThread, "createdBy" | "creationSource">) =>
  thread.createdBy === "agent" && thread.creationSource === "mcp";

const BUSY_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "preparing",
  "queued",
  "starting",
  "running",
  "waiting",
]);

/**
 * Why archiving now would cut work short, or `null` when the thread is at rest.
 * `thread.archive` itself cancels queued and preparing runs, so this has to
 * refuse before dispatch rather than rely on the orchestrator.
 */
export const busyReason = (
  shell: Pick<OrchestrationV2ThreadShell, "activeRunId" | "pendingBackgroundTasks">,
  projection: {
    readonly runs: ReadonlyArray<Pick<OrchestrationV2Run, "status">>;
    readonly runtimeRequests: ReadonlyArray<Pick<OrchestrationV2RuntimeRequest, "status">>;
  },
): string | null => {
  if (shell.activeRunId !== null || projection.runs.some((run) => run.status === "running"))
    return "is still running. Wait for it or interrupt it first.";
  if (projection.runtimeRequests.some((request) => request.status === "pending"))
    return "is waiting on an approval or question.";
  if (projection.runs.some((run) => BUSY_RUN_STATUSES.has(run.status)))
    return "has a queued or starting turn.";
  if ((shell.pendingBackgroundTasks?.length ?? 0) > 0) return "still has background work running.";
  return null;
};

const refuse = (message: string) =>
  new OrchestratorMcpFailure({ code: "capability_denied", message });

/**
 * Archives (`settle`) or restores (`unsettle`) a thread an agent launched,
 * through the same `thread.archive` / `thread.unarchive` commands the app's
 * archive button sends. Repeating either is a no-op that reports `changed: false`.
 */
export const setThreadArchived = Effect.fn("mcp.setThreadArchived")(function* (
  input: Pick<OrchestratorMcpThreadSettleInput, "threadId">,
  archived: boolean,
) {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  const { threads, projection, caller } = yield* readThread(input.threadId, [
    "runs",
    "runtimeRequests",
  ]);
  const thread = projection.thread;
  const result = (changed: boolean): OrchestratorMcpThreadSettleResult => ({
    threadId: thread.id,
    archived,
    changed,
    link: McpInvocationContext.threadLinkFor(scope, thread.id),
  });

  if (!isAgentLaunched(thread))
    return yield* refuse(
      `Thread ${thread.id} was not launched by an agent through T3's MCP tools. Only its user can ${archived ? "archive" : "restore"} it.`,
    );
  if (caller !== undefined && caller.id === thread.id)
    return yield* refuse("A thread cannot settle itself.");
  if ((thread.archivedAt !== null) === archived) return result(false);

  if (archived) {
    const shell = yield* threads.getThreadShell(thread.id).pipe(Effect.mapError(unavailable));
    if (shell === null || shell.deletedAt !== null)
      return yield* new OrchestratorMcpFailure({
        code: "thread_not_found",
        message: "The thread was not found.",
      });
    const busy = busyReason(shell, projection);
    if (busy !== null)
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: `Thread ${thread.id} ${busy} It was not settled.`,
      });
  }

  const commandId = yield* newCommandId();
  return yield* threads
    .dispatch({
      type: archived ? "thread.archive" : "thread.unarchive",
      commandId,
      threadId: thread.id,
    })
    .pipe(
      Effect.as(result(true)),
      // Another caller may have made the same change after the check above.
      Effect.catch((error) =>
        threads.getThreadShell(thread.id).pipe(
          Effect.orElseSucceed(() => null),
          Effect.flatMap((latest) =>
            latest !== null && (latest.archivedAt !== null) === archived
              ? Effect.succeed(result(false))
              : Effect.fail(dispatchFailure(error)),
          ),
        ),
      ),
    );
});
