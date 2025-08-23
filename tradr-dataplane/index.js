#!/usr/bin/env node
'use strict'

const assert = require('node:assert')
const { Database, DataPlaneServer } = require('/app/atproto/packages/bsky/dist/index.js')

const main = async () => {
  const env = getEnv()
  assert(env.dbPostgresUrl, 'must set BSKY_DATAPLANE_DB_POSTGRES_URL')
  
  console.log('Starting Bsky Data Plane Server...')
  console.log(`Database URL: ${env.dbPostgresUrl}`)
  console.log(`Database Schema: ${env.dbPostgresSchema}`)
  console.log(`PLC URL: ${env.didPlcUrl}`)
  console.log(`Port: ${env.port}`)
  console.log(`Run Migrations: ${env.dbMigrate}`)
  
  try {
    // Run migrations if enabled
    if (env.dbMigrate) {
      console.log('\nRunning database migrations...')
      
      // Separate migration db in case migration changes some connection state
      const migrationDb = new Database({
        url: env.dbPostgresUrl,
        schema: env.dbPostgresSchema,
        poolSize: 2, // Small pool for migration
      })
      
      const results = await migrationDb.migrateToLatestOrThrow()
      
      if (results && results.length > 0) {
        console.log(`Successfully ran ${results.length} migrations:`)
        results.forEach(result => {
          console.log(`  - ${result.migrationName}`)
        })
      } else {
        console.log('Database is already up to date')
      }
      
      await migrationDb.close()
    }
    
    // Create main database connection
    const db = new Database({
      url: env.dbPostgresUrl,
      schema: env.dbPostgresSchema,
      poolSize: env.dbPoolSize,
      poolMaxUses: env.dbPoolMaxUses,
      poolIdleTimeoutMs: env.dbPoolIdleTimeoutMs,
    })
    
    // Create and start the data plane server
    const dataplane = await DataPlaneServer.create(db, env.port, env.didPlcUrl)
    
    console.log(`✓ Data Plane Server listening on port ${env.port}`)
    
    // Graceful shutdown
    const shutdown = async () => {
      console.log('Shutting down Data Plane Server...')
      await dataplane.destroy()
      await db.close()
    }
    
    process.on('SIGTERM', shutdown)
    process.on('disconnect', shutdown) // when clustering
    
  } catch (error) {
    console.error('Failed to start Data Plane Server:', error)
    process.exit(1)
  }
}

const getEnv = () => ({
  port: parseInt(process.env.BSKY_DATAPLANE_PORT || '3000'),
  didPlcUrl: process.env.BSKY_DID_PLC_URL || 'https://plc.directory',
  dbPostgresUrl: process.env.BSKY_DATAPLANE_DB_POSTGRES_URL,
  dbPostgresSchema: process.env.BSKY_DATAPLANE_DB_POSTGRES_SCHEMA || 'bsky',
  dbMigrate: process.env.BSKY_DATAPLANE_DB_MIGRATE === '1',
  dbPoolSize: maybeParseInt(process.env.BSKY_DATAPLANE_DB_POOL_SIZE) || 10,
  dbPoolMaxUses: maybeParseInt(process.env.BSKY_DATAPLANE_DB_POOL_MAX_USES) || Infinity,
  dbPoolIdleTimeoutMs: maybeParseInt(process.env.BSKY_DATAPLANE_DB_POOL_IDLE_TIMEOUT_MS) || 10000,
})

const maybeParseInt = (str) => {
  if (!str) return
  const int = parseInt(str, 10)
  if (isNaN(int)) return
  return int
}

main()