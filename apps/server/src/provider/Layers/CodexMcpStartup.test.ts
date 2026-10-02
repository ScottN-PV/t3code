import * as NodeAssert from "node:assert/strict";

import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexSchema from "effect-codex-app-server/schema";

import { makeCodexMcpStartup } from "./CodexMcpStartup.ts";

type Client = CodexClient.CodexAppServerClient["Service"];
type Update = CodexSchema.V2McpServerStatusUpdatedNotification;

const makePeer = Effect.fnUntraced(function* (
  reload: Effect.Effect<unknown, CodexErrors.CodexAppServerError> = Effect.succeed({}),
) {
  let handler: (update: Update) => Effect.Effect<void, CodexErrors.CodexAppServerError> = () =>
    Effect.die("Startup handler was not registered");
  const requests: Array<string> = [];
  const reloads = yield* Queue.unbounded<void>();
  const client = {
    raw: {
      notifications: Stream.empty,
      requests: Stream.empty,
      request: () => Effect.die("Unexpected raw request"),
      notify: () => Effect.die("Unexpected raw notification"),
      respond: () => Effect.die("Unexpected raw response"),
      respondError: () => Effect.die("Unexpected raw error response"),
    },
    request: ((method) => {
      requests.push(method);
      // A status request starts a second copy of every server, so it must never be sent.
      NodeAssert.equal(method, "config/mcpServer/reload");
      return Queue.offer(reloads, undefined).pipe(Effect.andThen(reload));
    }) as Client["request"],
    handleServerNotification: ((method, onUpdate) =>
      Effect.sync(() => {
        NodeAssert.equal(method, "mcpServer/startupStatus/updated");
        handler = onUpdate as typeof handler;
      })) as Client["handleServerNotification"],
  };
  const refresh = yield* makeCodexMcpStartup().pipe(
    Effect.provide(Layer.mock(CodexClient.CodexAppServerClient)(client)),
  );
  const emit = (name: string, status: Update["status"], threadId = "root") =>
    handler({ threadId, name, status });
  // Forks a refresh and returns once its reload request is in flight.
  const startRefresh = (threadId = "root") =>
    refresh(threadId).pipe(
      Effect.forkChild,
      Effect.tap(() => Queue.take(reloads)),
    );
  return {
    startRefresh,
    refresh,
    awaitReload: Queue.take(reloads),
    requests,
    emit,
    emitUpdate: (update: Update) => handler(update),
  };
});

it.effect("a reused connection's ready update releases the turn after the quiet period", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready");
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "ready");
    yield* TestClock.adjust("100 millis");
    yield* Fiber.join(turn);
    NodeAssert.deepStrictEqual(peer.requests, ["config/mcpServer/reload"]);
  }),
);

it.effect("keeps a reused connection's ready update received before the reload reply", () =>
  Effect.gen(function* () {
    const reloading = yield* Deferred.make<void>();
    const peer = yield* makePeer(Deferred.await(reloading));
    yield* peer.emit("tools", "ready");
    const turn = yield* peer.startRefresh();
    yield* TestClock.adjust("1 second");
    yield* peer.emit("tools", "ready");
    yield* Deferred.succeed(reloading, undefined);
    yield* TestClock.adjust("100 millis");
    yield* Fiber.join(turn);
  }),
);

it.effect("waits for a server still starting from thread start", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("slow", "starting");
    const turn = yield* peer.startRefresh();
    yield* TestClock.adjust("20 seconds");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* peer.emit("slow", "ready");
    yield* Fiber.join(turn);
  }),
);

it.effect("waits for a restarted server and lets a later ready win over cancelled", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready");
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "cancelled");
    yield* TestClock.adjust("20 seconds");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* peer.emit("tools", "ready");
    yield* Fiber.join(turn);
  }),
);

it.effect("a reload that reports nothing releases the turn after the grace and quiet period", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready");
    const turn = yield* peer.startRefresh();
    yield* TestClock.adjust("599 millis");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* TestClock.adjust("1 milli");
    yield* Fiber.join(turn);
  }),
);

it.effect("stops waiting for a hung server after one timeout until it recovers", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("hung", "starting");
    const first = yield* peer.startRefresh();
    yield* TestClock.adjust("29 seconds");
    NodeAssert.equal(first.pollUnsafe(), undefined);
    yield* TestClock.adjust("1 second");
    yield* Fiber.join(first);

    // The next reload restarts the failed server; the turn does not wait for it again.
    const second = yield* peer.startRefresh();
    yield* peer.emit("hung", "starting");
    yield* TestClock.adjust("100 millis");
    yield* Fiber.join(second);

    // After it recovers, a later restart is waited for again.
    yield* peer.emit("hung", "ready");
    const third = yield* peer.startRefresh();
    yield* peer.emit("hung", "starting");
    yield* TestClock.adjust("1 second");
    NodeAssert.equal(third.pollUnsafe(), undefined);
    yield* peer.emit("hung", "ready");
    yield* Fiber.join(third);
  }),
);

it.effect("waits for a server whose starting update arrives later in the same burst", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("flaky", "starting");
    yield* peer.emit("flaky", "failed");
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready");
    const turn = yield* peer.startRefresh();
    // A failed server's restart arrives first and is not waited for; the restarted
    // tools server reports starting right after it.
    yield* peer.emit("flaky", "starting");
    yield* TestClock.adjust("50 millis");
    yield* peer.emit("tools", "starting");
    yield* TestClock.adjust("20 seconds");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* peer.emit("tools", "ready");
    yield* Fiber.join(turn);
  }),
);

it.effect("a late cancelled update after ready does not block the turn", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready");
    yield* peer.emit("tools", "cancelled");
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "ready");
    yield* TestClock.adjust("100 millis");
    yield* Fiber.join(turn);
  }),
);

it.effect("a failed server does not block the remaining servers", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    const turn = yield* peer.startRefresh();
    yield* peer.emit("failed", "starting");
    yield* peer.emit("ready", "starting");
    yield* peer.emit("failed", "failed");
    yield* TestClock.adjust("1 second");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* peer.emit("ready", "ready");
    yield* Fiber.join(turn);
  }),
);

it.effect("ignores other threads' and unscoped updates", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "starting");
    yield* peer.emit("tools", "ready", "child");
    yield* peer.emitUpdate({ name: "tools", status: "ready" });
    yield* TestClock.adjust("20 seconds");
    NodeAssert.equal(turn.pollUnsafe(), undefined);
    yield* peer.emit("tools", "ready");
    yield* Fiber.join(turn);
  }),
);

it.effect("other threads' updates do not extend the quiet period", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "ready");
    for (let update = 0; update < 4; update++) {
      yield* TestClock.adjust("25 millis");
      yield* peer.emit("tools", "starting", "child");
    }
    yield* Fiber.join(turn);
  }),
);

it.effect("observes an own update coalesced with another thread's update", () =>
  Effect.gen(function* () {
    const reloading = yield* Deferred.make<void>();
    const peer = yield* makePeer(Deferred.await(reloading));
    const turn = yield* peer.startRefresh();
    yield* peer.emit("tools", "ready");
    yield* peer.emit("tools", "ready", "child");
    yield* Deferred.succeed(reloading, undefined);
    yield* TestClock.adjust("100 millis");
    yield* Fiber.join(turn);
  }),
);

it.effect("serializes overlapping refreshes, each with its own reload", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer();
    yield* peer.emit("slow", "starting");
    const first = yield* peer.startRefresh();
    const second = yield* peer.refresh("root").pipe(Effect.forkChild);
    yield* TestClock.adjust("1 second");
    NodeAssert.deepStrictEqual(peer.requests, ["config/mcpServer/reload"]);
    yield* peer.emit("slow", "ready");
    yield* Fiber.join(first);
    yield* peer.awaitReload;
    yield* TestClock.adjust("600 millis");
    yield* Fiber.join(second);
    NodeAssert.deepStrictEqual(peer.requests, [
      "config/mcpServer/reload",
      "config/mcpServer/reload",
    ]);
  }),
);

it.effect("continues when the reload fails", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer(
      Effect.fail(CodexErrors.CodexAppServerRequestError.methodNotFound("config/mcpServer/reload")),
    );
    yield* peer.refresh("root");
    NodeAssert.deepStrictEqual(peer.requests, ["config/mcpServer/reload"]);
  }),
);

it.effect("bounds a stuck reload request", () =>
  Effect.gen(function* () {
    const peer = yield* makePeer(Effect.never);
    const turn = yield* peer.startRefresh();
    yield* TestClock.adjust("30 seconds");
    yield* Fiber.join(turn);
  }),
);
