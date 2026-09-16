import pg from 'pg';

async function main() {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  const client = await pool.connect();
  
  await client.query('ALTER TABLE api.evm_transactions ADD COLUMN IF NOT EXISTS decoded_args JSONB');
  console.log('Added decoded_args column');
  
  await client.query('CREATE INDEX IF NOT EXISTS idx_evm_tx_contract_address ON api.evm_transactions(contract_address) WHERE contract_address IS NOT NULL');
  console.log('Created index on contract_address');
  
  await client.query('CREATE INDEX IF NOT EXISTS idx_evm_contracts_creator ON api.evm_contracts(creator)');
  console.log('Created index on evm_contracts.creator');
  
  client.release();
  await pool.end();
}

main().catch(console.error);
