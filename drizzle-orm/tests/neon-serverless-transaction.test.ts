/* eslint-disable drizzle-internal/require-entity-kind */
import { Pool, type PoolClient } from '@neondatabase/serverless';
import { describe, expect, test } from 'vitest';
import { drizzle } from '~/neon-serverless/index.ts';
import { sql } from '~/sql/sql.ts';

/**
 * In-memory stand-in for a Neon `PoolClient`. No socket is opened.
 * `release` records its arguments so tests can see whether the session
 * returned the client to the pool or asked the pool to discard it.
 */
class ScriptedClient {
	readonly queries: string[] = [];
	readonly releases: unknown[][] = [];

	/** When set, `BEGIN` fails the way a bad Neon password does (28P01). */
	failBegin = false;

	query(config: string | { text?: string }): { rows: unknown[] } {
		const text = typeof config === 'string' ? config : config.text ?? '';
		this.queries.push(text);
		if (this.failBegin && /^\s*begin\b/i.test(text)) {
			throw Object.assign(new Error('password authentication failed'), { code: '28P01' });
		}
		// A follow-up after a failed BEGIN is a dead connection, not another 28P01.
		if (this.failBegin && /^\s*rollback\b/i.test(text)) {
			throw Object.assign(new Error('Connection terminated unexpectedly'), { code: '57P01' });
		}
		return { rows: [] };
	}

	release(...args: unknown[]) {
		this.releases.push(args);
	}
}

class ScriptedPool extends Pool {
	constructor(private readonly scripted: ScriptedClient) {
		// Port 1 is closed. `connect` is overridden, so this never dials out.
		super({ connectionString: 'postgres://user:pass@127.0.0.1:1/neondb' });
	}

	override connect(): Promise<PoolClient> {
		return Promise.resolve(this.scripted as unknown as PoolClient);
	}
}

describe('neon-serverless transaction pool client', () => {
	test('releases and discards the client when BEGIN fails', async () => {
		const scripted = new ScriptedClient();
		scripted.failBegin = true;
		const pool = new ScriptedPool(scripted);
		const db = drizzle(pool);
		let callbackRan = false;

		const error = await db.transaction(async () => {
			callbackRan = true;
		}).then(() => {
			throw new Error('transaction resolved');
		}, (cause: unknown) => cause);

		expect(callbackRan).toBe(false);
		expect(error).toMatchObject({ cause: { code: '28P01' } });
		expect(scripted.queries.some((query) => /^\s*rollback\b/i.test(query))).toBe(false);
		expect(scripted.queries.some((query) => /^\s*begin\b/i.test(query))).toBe(true);
		expect(scripted.releases).toHaveLength(1);
		expect(scripted.releases[0]?.[0]).toBe(error);

		await pool.end();
	});

	test('releases the client without discarding it when a query inside the transaction fails', async () => {
		const scripted = new ScriptedClient();
		const pool = new ScriptedPool(scripted);
		const db = drizzle(pool);

		await expect(db.transaction(async () => {
			throw new Error('query failed');
		})).rejects.toThrow('query failed');

		expect(scripted.queries.some((query) => /^\s*begin\b/i.test(query))).toBe(true);
		expect(scripted.queries.some((query) => /^\s*rollback\b/i.test(query))).toBe(true);
		expect(scripted.releases).toEqual([[]]);

		await pool.end();
	});

	test('commits and releases the client once when the transaction succeeds', async () => {
		const scripted = new ScriptedClient();
		const pool = new ScriptedPool(scripted);
		const db = drizzle(pool);

		await expect(db.transaction(async (tx) => {
			await tx.execute(sql`select 1`);
			return 'ok';
		})).resolves.toBe('ok');

		expect(scripted.queries.map((query) => query.trim().split(/\s+/)[0]?.toLowerCase())).toEqual([
			'begin',
			'select',
			'commit',
		]);
		expect(scripted.releases).toEqual([[]]);

		await pool.end();
	});
});
