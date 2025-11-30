import pg from 'pg';

async function main() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  const client = await pool.connect();
  
  console.log('Dropping old function overload...');
  await client.query(`
    DROP FUNCTION IF EXISTS api.get_transactions_paginated(integer, integer, text, bigint, text)
  `);
  console.log('Dropped old function overload successfully!');
  
  client.release();
  await pool.end();
}

main().catch(console.error);
