import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Run,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/ai";

import { OrchestratorDispatchError } from "../orchestration-v2/Orchestrator.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as RemoteDelegation from "../peer/RemoteDelegation.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpToolAccessTestkit from "./McpToolAccess.testkit.ts";
import { busyReason } from "./threadSettlement.ts";

interface FakeThread {
  readonly createdBy: "user" | "agent";
  readonly creationSource: "web" | "mcp";
  archivedAt: DateTime.Utc | null;
  readonly activeRunId?: RunId | null;
  readonly runs?: ReadonlyArray<Pick<OrchestrationV2Run, "status">>;
  readonly pendingRequest?: boolean;
  /** Archived by someone else just before this dispatch lands. */
  readonly raced?: boolean;
}

const callerThreadId = ThreadId.make("settle-caller");
const environmentId = EnvironmentId.make("settle-environment");

const threadScope: McpInvocationContext.McpInvocationScope = {
  environmentId,
  requestNamespace: "settle-session",
  thread: {
    threadId: callerThreadId,
    providerSessionId: "settle-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
};

const clientScope = (
  access: McpInvocationContext.McpClientCaller["access"],
): McpInvocationContext.McpInvocationScope => ({
  environmentId,
  requestNamespace: "client:settle",
  thread: undefined,
  client: { sessionId: "settle-client", label: "Claude Code", access },
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
});

const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "settle-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "settle-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const declaredFailure = (result: McpSchema.CallToolResult) => {
  const text = result.content[0];
  return result.isError === true && text?.type === "text" ? JSON.parse(text.text) : undefined;
};

const launched = (overrides: Partial<FakeThread> = {}): FakeThread => ({
  createdBy: "agent",
  creationSource: "mcp",
  archivedAt: null,
  activeRunId: null,
  runs: [{ status: "completed" }],
  ...overrides,
});

const harness = (threads: Record<string, FakeThread>) => {
  const dispatched: Array<OrchestrationV2ServerCommand["type"]> = [];
  const shellOf = (id: ThreadId) => {
    if (id === callerThreadId) return McpToolAccessTestkit.liveThreadShell(id);
    const thread = threads[id];
    if (thread === undefined) return null;
    return {
      ...McpToolAccessTestkit.liveThreadShell(id, {
        activeRunId: thread.activeRunId ?? null,
      }),
      createdBy: thread.createdBy,
      creationSource: thread.creationSource,
      archivedAt: thread.archivedAt,
    };
  };
  const layer = McpHttpServer.layerOrchestratorToolkit.pipe(
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(NodeCrypto.layer),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (id) => Effect.succeed(shellOf(id)),
        getProjectThreadRecords: ({ threadId }) => {
          const thread = threads[threadId];
          const shell = shellOf(threadId);
          return Effect.succeed({
            thread: {
              ...shell,
              createdBy: thread?.createdBy ?? "agent",
              creationSource: thread?.creationSource ?? "mcp",
            },
            runs: thread?.runs ?? [],
            runtimeRequests: thread?.pendingRequest === true ? [{ status: "pending" }] : [],
          } as never);
        },
        dispatch: (command) =>
          Effect.gen(function* () {
            const thread = "threadId" in command ? threads[command.threadId] : undefined;
            if (thread === undefined) return yield* Effect.die("unknown thread");
            if (thread.raced === true) {
              thread.archivedAt = DateTime.makeUnsafe("2026-10-31T00:00:00.000Z");
              return yield* new OrchestratorDispatchError({
                commandId: CommandId.make(command.commandId),
                commandType: command.type,
                cause: "Thread is already archived.",
              });
            }
            dispatched.push(command.type);
            thread.archivedAt =
              command.type === "thread.archive"
                ? DateTime.makeUnsafe("2026-10-31T00:00:00.000Z")
                : null;
            return { sequence: dispatched.length, storedEvents: [] };
          }),
      }),
    ),
    Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(Layer.mock(ProviderRegistry.ProviderRegistry)({})),
    Layer.provide(Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({})),
    Layer.provide(Layer.mock(ScheduledTaskService.ScheduledTaskService)({})),
    Layer.provide(Layer.mock(ProjectService.ProjectService)({})),
    Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
    Layer.provide(Layer.mock(RemoteDelegation.RemoteDelegation)({})),
    Layer.provide(Layer.mock(PeerForwarding.PeerForwarding)({})),
    Layer.provide(Layer.mock(PeerLinkRequests.PeerLinkRequests)({})),
    Layer.provide(Layer.mock(PeerLinks.PeerLinks)({})),
  );
  const call = (
    name: "t3_thread_settle" | "t3_thread_unsettle",
    threadId: string,
    scope: McpInvocationContext.McpInvocationScope = threadScope,
  ) =>
    McpServer.McpServer.pipe(
      Effect.flatMap((server) => server.callTool({ name, arguments: { threadId } })),
      Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
      Effect.provideService(McpSchema.McpServerClient, mcpClient),
    );
  return { dispatched, layer, call };
};

it.effect("settles a finished launched thread once and restores it once", () => {
  const { dispatched, layer, call } = harness({ child: launched() });
  return Effect.gen(function* () {
    const settled = yield* call("t3_thread_settle", "child");
    expect(settled.isError).toBe(false);
    expect(settled.structuredContent).toMatchObject({
      threadId: "child",
      archived: true,
      changed: true,
    });

    const again = yield* call("t3_thread_settle", "child");
    expect(again.structuredContent).toMatchObject({ archived: true, changed: false });

    const restored = yield* call("t3_thread_unsettle", "child");
    expect(restored.structuredContent).toMatchObject({ archived: false, changed: true });

    const restoredAgain = yield* call("t3_thread_unsettle", "child");
    expect(restoredAgain.structuredContent).toMatchObject({ archived: false, changed: false });

    expect(dispatched).toEqual(["thread.archive", "thread.unarchive"]);
  }).pipe(Effect.provide(layer));
});

it.effect("never settles or restores a thread the user started", () => {
  const archivedAt = DateTime.makeUnsafe("2026-10-30T00:00:00.000Z");
  const { dispatched, layer, call } = harness({
    mine: launched({ createdBy: "user", creationSource: "web" }),
    mineArchived: launched({ createdBy: "user", creationSource: "web", archivedAt }),
    agentInApp: launched({ createdBy: "agent", creationSource: "web" }),
  });
  return Effect.gen(function* () {
    for (const [tool, id] of [
      ["t3_thread_settle", "mine"],
      ["t3_thread_settle", "mineArchived"],
      ["t3_thread_unsettle", "mineArchived"],
      ["t3_thread_settle", "agentInApp"],
    ] as const) {
      const result = yield* call(tool, id);
      expect(declaredFailure(result), `${tool} ${id}`).toMatchObject({
        code: "capability_denied",
      });
    }
    expect(dispatched).toEqual([]);
  }).pipe(Effect.provide(layer));
});

it.effect("refuses to settle a thread that is running, queued, or waiting on a question", () => {
  const { dispatched, layer, call } = harness({
    running: launched({ activeRunId: RunId.make("run:live"), runs: [{ status: "running" }] }),
    queued: launched({ runs: [{ status: "completed" }, { status: "queued" }] }),
    waiting: launched({ runs: [{ status: "waiting" }] }),
    asking: launched({ pendingRequest: true }),
  });
  return Effect.gen(function* () {
    for (const id of ["running", "queued", "waiting", "asking"]) {
      const result = yield* call("t3_thread_settle", id);
      expect(declaredFailure(result), id).toMatchObject({ code: "invalid_request" });
    }
    expect(dispatched).toEqual([]);
  }).pipe(Effect.provide(layer));
});

it.effect("refuses read-only clients and a thread settling itself", () => {
  const { dispatched, layer, call } = harness({
    child: launched(),
    [callerThreadId]: launched(),
  });
  return Effect.gen(function* () {
    const readOnly = yield* call("t3_thread_settle", "child", clientScope("read-only"));
    expect(declaredFailure(readOnly)).toMatchObject({ code: "capability_denied" });
    const readOnlyRestore = yield* call("t3_thread_unsettle", "child", clientScope("read-only"));
    expect(declaredFailure(readOnlyRestore)).toMatchObject({ code: "capability_denied" });

    const itself = yield* call("t3_thread_settle", callerThreadId);
    expect(declaredFailure(itself)).toMatchObject({ code: "capability_denied" });
    expect(dispatched).toEqual([]);

    const fullClient = yield* call("t3_thread_settle", "child", clientScope("full-access"));
    expect(fullClient.structuredContent).toMatchObject({ archived: true, changed: true });
  }).pipe(Effect.provide(layer));
});

it.effect("treats a settle that loses a race to another archive as already done", () => {
  const { layer, call } = harness({ child: launched({ raced: true }) });
  return Effect.gen(function* () {
    const result = yield* call("t3_thread_settle", "child");
    expect(result.isError).toBe(false);
    expect(result.structuredContent).toMatchObject({ archived: true, changed: false });
  }).pipe(Effect.provide(layer));
});

it("reports background work as busy", () => {
  expect(
    busyReason(
      {
        activeRunId: null,
        pendingBackgroundTasks: [{ kind: "subagent" } as never],
      },
      { runs: [], runtimeRequests: [] },
    ),
  ).toContain("background work");
  expect(
    busyReason({ activeRunId: null }, { runs: [{ status: "completed" }], runtimeRequests: [] }),
  ).toBeNull();
});
