// Build the Database layer against the file in argv[2], which runs migrations, then exit. Used by
// the cross-process migration test to race several independent processes against one shared file.
import { Effect, Layer } from "effect"
import { Database } from "../src/database/database"

const file = process.argv[2]
if (!file) {
  console.error("usage: migrate-once.ts <db-file>")
  process.exit(2)
}

Effect.runPromise(Effect.scoped(Layer.build(Database.layerFromPath(file))))
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error)
    process.exit(1)
  })
