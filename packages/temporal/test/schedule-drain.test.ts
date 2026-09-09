import { expect } from "bun:test"
import { Effect } from "effect"
import { eq } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionInputTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { testEffect } from "../../core/test/lib/effect"
import { makeScheduleDrains } from "../src/l2-drain"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node]), [
    [Database.node, Database.layerFromPath(":memory:")],
  ]),
)

it.effect("admits later schedule firings without taking the runner's owner", () =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const sessionID = SessionSchema.ID.make("ses_schedule")
    yield* db
      .insert(ProjectTable)
      .values({
        id: Project.ID.global,
        worktree: AbsolutePath.make("/project"),
        sandboxes: [],
      })
      .run()
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "schedule",
        directory: "/project",
        title: "schedule",
        version: "test",
      })
      .run()
    const { promptDrain } = makeScheduleDrains({ db, events })
    const first = { sessionID, messageID: "msg_schedule_first", text: "check the project" }
    yield* Effect.promise(() => promptDrain(first))
    yield* events.claim(sessionID, "run:1:1")
    yield* Effect.promise(() => promptDrain({ ...first, messageID: "msg_schedule_second" }))
    yield* Effect.promise(() => promptDrain(first))

    const prompts = yield* db.select().from(SessionInputTable).all()
    const owner = yield* db
      .select()
      .from(EventSequenceTable)
      .where(eq(EventSequenceTable.aggregate_id, sessionID))
      .get()
    expect(prompts.map((row) => String(row.id)).sort()).toEqual(["msg_schedule_first", "msg_schedule_second"])
    expect(owner?.owner_id).toBe("run:1:1")
  }),
)
