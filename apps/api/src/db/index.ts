import 'dotenv/config';
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export const db = drizzle(pool);

export async function testDatabaseConnection() {
  const result = await pool.query('SELECT NOW()');

  console.log('Database connected:', result.rows[0]);
}