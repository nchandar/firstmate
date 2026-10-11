// Firstmate semantic busy-state events + turn-end notification; written by
// fm-spawn under the contract owned by bin/fm-busy-lib.sh.
// Semantic state comes from OpenCode 2's session.execution.started and terminal
// session.execution.* events, with the OpenCode 1 session.status/session.idle
// names retained as legacy inputs. Scoping latches the first session that
// reports activity and ignores other sessions until the latched session settles,
// so a child's terminal event can never clear the worker's busy state. The
// turn-ended touch stays the watcher's wake NOTIFICATION, never current-state
// truth.
import { execFile } from "node:child_process";
const notifyTurnEnd = true;
const busyEvent = (state, event) =>
  new Promise((resolve) => {
    execFile("/Users/niranjanchandarraj/.no-mistakes/worktrees/202df2ceeeaf/01M4P5B8D7VE3W9QRY6NQB2H2T/bin/fm-busy-event.sh", [
      "apply", "/private/var/folders/nq/kgpf74px1rn116q_x77l6g8m0000gn/T/fm-busy-adapter-wiring.oopr3P/oc-gen-evid/home/state", "busy-oc-gen", state,
      "--gen", "g1791746893.17722.19632", "--source", "opencode-plugin", "--event", event,
    ], () => resolve());
  });
const touchTurnEnded = () =>
  new Promise((resolve) => {
    execFile("touch", ["/private/var/folders/nq/kgpf74px1rn116q_x77l6g8m0000gn/T/fm-busy-adapter-wiring.oopr3P/oc-gen-evid/home/state/busy-oc-gen.turn-ended"], () => resolve());
  });
const eventType = (event) => event && (event.type || event.name) || "";
const eventProperties = (event) => event && (event.properties || event.data) || {};
const sessionID = (event) => {
  const properties = eventProperties(event);
  return properties.sessionID || properties.sessionId || (properties.session && (properties.session.id || properties.session.parentID || properties.session.parentId)) || properties.id || "";
};
const setupEventSubscription = (ctx, handler) => {
  if (!ctx || !ctx.event || !ctx.event.subscribe) return false;
  const controller = new AbortController();
  void (async () => {
    try {
      const stream = await ctx.event.subscribe({ signal: controller.signal, onActivity() {} });
      for await (const event of stream) await handler(event);
    } catch {
    }
  })();
  return true;
};
const makeHandler = () => {
  let activeSession = null;
  return async (event) => {
    const type = eventType(event);
    const id = sessionID(event);
    if (type === "session.execution.started") {
      if (activeSession === null) activeSession = id;
      if (id === activeSession) await busyEvent("busy", "session-execution-started");
      return;
    }
    if (type === "session.execution.succeeded" || type === "session.execution.interrupted" || type === "session.execution.failed") {
      if (id === activeSession) {
        activeSession = null;
        await busyEvent("idle", type.replace(/\./g, "-"));
      }
      if (notifyTurnEnd) await touchTurnEnded();
      return;
    }
    if (type === "session.status") {
      const properties = eventProperties(event);
      const statusType = properties.status && properties.status.type;
      if (statusType === "busy" || statusType === "retry") {
        if (activeSession === null) activeSession = id;
        if (id === activeSession) await busyEvent("busy", "session-" + statusType);
        return;
      }
      if (statusType === "idle" && id === activeSession) {
        activeSession = null;
        await busyEvent("idle", "session-status-idle");
      }
      return;
    }
    if (type === "session.idle") {
      if (id === activeSession) {
        activeSession = null;
        await busyEvent("idle", "session-idle");
      }
      if (notifyTurnEnd) await touchTurnEnded();
    }
  };
};
export const FmBusyState = async () => {
  const handleEvent = makeHandler();
  return {
    event: async ({ event }) => handleEvent(event),
  };
};

export default {
  id: "fm.busy-state",
  server: FmBusyState,
  setup(ctx) {
    setupEventSubscription(ctx, makeHandler());
  },
};
