import {
  inspectDefaultSubagentTaskBacking,
  isDefaultDetachedTaskLifecycleRuntime,
} from "../../../tasks/detached-task-runtime.js";
import type { SubagentTaskBackingPolicy } from "../../../tasks/task-backing-authority.js";
import type { TaskRecord } from "../../../tasks/task-registry.types.js";
import type { SubagentRunRecord, SubagentTaskOwnershipPolicy } from "./subagent-registry.types.js";

export function resolveNewSubagentTaskOwnershipPolicy(params: {
  taskRowOwnership: "required" | "gateway_best_effort";
  usesDefaultRuntime: boolean;
}): SubagentTaskOwnershipPolicy {
  if (params.taskRowOwnership === "gateway_best_effort") {
    return "gateway_best_effort";
  }
  return params.usesDefaultRuntime ? "core_required" : "custom";
}

export type SubagentTaskOwnershipInspection =
  | { kind: "core_required"; task: TaskRecord }
  | { kind: "gateway_best_effort" }
  | { kind: "custom" }
  | { kind: "invalid"; reason: string };

/** Resolves only persisted ownership policy; legacy rows never infer authority from nearby tasks. */
export function inspectSubagentTaskOwnership(params: {
  entry: SubagentRunRecord;
  backingPolicy: SubagentTaskBackingPolicy;
}): SubagentTaskOwnershipInspection {
  const policy = params.entry.taskOwnershipPolicy;
  if (!policy) {
    return { kind: "invalid", reason: "has no persisted task ownership policy" };
  }
  if (policy === "gateway_best_effort") {
    return { kind: "gateway_best_effort" };
  }
  if (policy === "custom") {
    return isDefaultDetachedTaskLifecycleRuntime()
      ? { kind: "invalid", reason: "requires its registered custom task runtime" }
      : { kind: "custom" };
  }
  const backing = inspectDefaultSubagentTaskBacking({
    runId: params.entry.taskRunId ?? params.entry.runId,
    ownerKey: params.entry.requesterSessionKey,
    sessionKey: params.entry.childSessionKey,
    generation: params.entry.generation,
    policy: params.backingPolicy,
  });
  return backing.kind === "valid"
    ? { kind: "core_required", task: backing.task }
    : {
        kind: "invalid",
        reason:
          backing.kind === "custom"
            ? "requires the default task runtime"
            : `task backing ${backing.reason}`,
      };
}
