'use strict';

// Records wire-protocol fixtures from @owservable/core v3 (TypeScript) for the Rust parity tests.
// Each scenario drives an OwservableClient against an in-memory backend and stores, per step,
// every message the client emitted. The Rust tests replay the same steps against the same
// in-memory backend semantics and compare outputs.

const fs = require('node:fs');
const path = require('node:path');
const {Subject} = require('rxjs');
const siftModule = require('sift');
const {OwservableClient, BackendRegistry, DataMiddlewareMap} = require('@owservable/core');

const sift = siftModule.default || siftModule;
const OUT_DIR = path.resolve(__dirname, '../../crates/core/tests/golden');
const SETTLE_MS = 300;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clone = (value) => JSON.parse(JSON.stringify(value));

// Backend semantics the Rust test double must mirror: sift matching, multi-key sort, skip/limit,
// no projection, populate/virtuals are no-ops.
class MemoryBackend {
	constructor(collection, docs) {
		this.collection = collection;
		this.docs = clone(docs);
		this.subject = new Subject();
	}

	target() {
		return this.collection;
	}

	changes() {
		return this.subject;
	}

	async find(query, fields, paging, sort) {
		let docs = this.docs.filter(sift(query));
		const keys = Object.entries(sort || {});
		docs.sort((a, b) => {
			for (const [key, dir] of keys) {
				if (a[key] < b[key]) return -dir;
				if (a[key] > b[key]) return dir;
			}
			return 0;
		});
		const skip = (paging && paging.skip) || 0;
		const limit = paging && paging.limit;
		docs = docs.slice(skip, limit ? skip + limit : undefined);
		return clone(docs);
	}

	async findOne(query) {
		const doc = this.docs.find(sift(query));
		return doc ? clone(doc) : null;
	}

	async findById(id) {
		const doc = this.docs.find((d) => d._id === id);
		return doc ? clone(doc) : null;
	}

	async count(query) {
		return this.docs.filter(sift(query)).length;
	}

	async populate(document) {
		return document;
	}

	toJSON(document) {
		return document;
	}

	async resolveVirtuals(document) {
		return document;
	}

	apply(change) {
		const ns = {db: 'test', coll: this.collection};
		if (change.op === 'insert') {
			this.docs.push(clone(change.doc));
			this.subject.next({ns, documentKey: {_id: change.doc._id}, operationType: 'insert', fullDocument: clone(change.doc)});
		} else if (change.op === 'update') {
			const doc = this.docs.find((d) => d._id === change.id);
			Object.assign(doc, change.set);
			this.subject.next({
				ns,
				documentKey: {_id: change.id},
				operationType: 'update',
				updateDescription: {updatedFields: clone(change.set), removedFields: []},
				fullDocument: clone(doc)
			});
		} else if (change.op === 'delete') {
			this.docs = this.docs.filter((d) => d._id !== change.id);
			this.subject.next({ns, documentKey: {_id: change.id}, operationType: 'delete'});
		} else {
			throw new Error(`unknown change op ${change.op}`);
		}
	}
}

const connectionManager = {
	user: () => ({id: 'u1'}),
	connected: () => undefined,
	ping: () => undefined,
	location: () => undefined,
	disconnected: () => undefined,
	checkSession: async () => ({type: 'refresh', payload: {jwt: 'refreshed-jwt', user: {id: 'u1'}}, refresh_in: 600000})
};

// Strip non-deterministic values; the Rust tests apply the same normalization.
const normalize = (message) => {
	const m = clone(message);
	if (m.execution_time !== undefined) m.execution_time = '<ms>';
	if ((m.type === 'debug' || m.type === 'ping') && m.id !== undefined) m.id = 0;
	return m;
};

const DOCS = [
	{_id: 'a', name: 'Alpha', status: 'open', n: 1},
	{_id: 'b', name: 'Bravo', status: 'open', n: 2},
	{_id: 'c', name: 'Charlie', status: 'closed', n: 3},
	{_id: 'd', name: 'Delta', status: 'open', n: 4}
];

const sub = (target, scope, observe, config) => ({input: {type: 'subscribe', target, scope, observe, config}});
const cfg = (subscriptionId, extra) => ({subscriptionId, strict: false, incremental: false, ...extra});
const insert = (doc) => ({changes: [{op: 'insert', doc}]});
const update = (id, set) => ({changes: [{op: 'update', id, set}]});
const remove = (id) => ({changes: [{op: 'delete', id}]});
const wait = (ms) => ({wait: ms});
const unsubscribe = (target) => ({input: {type: 'unsubscribe', target}});
const reload = (target) => ({input: {type: 'reload', target}});
const disconnect = {disconnect: true};

const SCENARIOS = {
	'many-basic': [
		sub('list', 'many', 'items', cfg('s1', {query: {status: 'open'}, sort: {n: 1}})),
		insert({_id: 'e', name: 'Echo', status: 'open', n: 5}),
		update('c', {name: 'Charlie 2'}),
		update('a', {name: 'Alpha 2'}),
		wait(2000), // count throttled: nothing yet
		wait(3200), // trailing total, 5s after the leading one
		remove('b'),
		wait(5200),
		unsubscribe('list'),
		disconnect
	],
	'many-paged-fields': [
		sub('list', 'many', 'items', cfg('s1', {query: {status: 'open'}, sort: {n: 1}, fields: ['name'], page: 2, pageSize: 2})),
		update('d', {n: 9}),
		update('d', {name: 'Delta 2'}),
		wait(5200),
		disconnect
	],
	'many-incremental': [
		sub('list', 'many', 'items', cfg('s1', {query: {status: 'open'}, incremental: true})),
		insert({_id: 'e', name: 'Echo', status: 'open', n: 5}),
		update('a', {status: 'closed'}),
		remove('b'),
		wait(5200),
		disconnect
	],
	'many-requery': [
		sub('list', 'many', 'items', cfg('s1', {query: {status: 'open'}})),
		sub('list', 'many', 'items', cfg('s2', {query: {status: 'closed'}})),
		sub('list', 'many', 'items', cfg('s2', {query: {status: 'closed'}})),
		reload('list'),
		wait(5200),
		disconnect
	],
	'many-burst': [
		sub('list', 'many', 'items', cfg('s1', {query: {status: 'open'}})),
		{changes: [{op: 'update', id: 'a', set: {name: 'A1'}}, {op: 'update', id: 'a', set: {name: 'A2'}}, {op: 'update', id: 'a', set: {name: 'A3'}}]},
		wait(5200),
		disconnect
	],
	'one-by-id': [
		sub('doc', 'one', 'items', cfg('s1', {query: {_id: 'b'}})),
		update('b', {name: 'Bravo 2'}),
		update('c', {name: 'Charlie 2'}),
		remove('b'),
		disconnect
	],
	'one-sorted': [
		sub('top', 'one', 'items', cfg('s1', {query: {status: 'open'}, sort: {n: -1}})),
		insert({_id: 'e', name: 'Echo', status: 'open', n: 5}),
		update('c', {name: 'Charlie 2'}),
		disconnect
	],
	count: [
		sub('total', 'count', 'items', cfg('s1', {query: {status: 'open'}})),
		insert({_id: 'e', name: 'Echo', status: 'open', n: 5}),
		update('a', {name: 'Alpha 2'}),
		update('c', {status: 'open'}),
		remove('b'),
		disconnect
	],
	session: [
		{input: {type: 'authenticate', jwt: 'initial-jwt'}},
		{input: {type: 'location', path: '/dashboard'}},
		{input: {type: 'pong', id: 0}},
		disconnect
	],
	'unknown-observe': [sub('list', 'many', 'nope', cfg('s1', {query: {}})), disconnect],
	middleware: [sub('list', 'many', 'tagged', cfg('s1', {query: {status: 'open'}})), update('a', {name: 'Alpha 2'}), wait(5200), disconnect]
};

const runScenario = async (steps) => {
	BackendRegistry.clear();
	const backends = {items: new MemoryBackend('items', DOCS), tagged: new MemoryBackend('tagged', DOCS)};
	for (const [observe, backend] of Object.entries(backends)) BackendRegistry.register(observe, backend);

	const client = new OwservableClient(connectionManager);
	let outputs = [];
	client.subscribe({
		next: (m) => outputs.push(normalize(m)),
		error: (e) => outputs.push({error: String(e && e.message ? e.message : e)})
	});

	const recorded = [];
	for (const step of steps) {
		outputs = [];
		if (step.input) {
			try {
				await client.consume(clone(step.input));
			} catch (e) {
				outputs.push({thrown: e.message});
			}
		}
		if (step.changes) {
			// both backends hold identical data; each scenario subscribes to only one of them
			for (const change of step.changes) for (const backend of Object.values(backends)) backend.apply(change);
		}
		if (step.disconnect) client.disconnected();

		await sleep(step.wait || SETTLE_MS);
		recorded.push({...step, outputs});
	}
	return recorded;
};

const main = async () => {
	DataMiddlewareMap.addMiddleware('tagged', (m) => ({...m, tagged: true}));
	fs.mkdirSync(OUT_DIR, {recursive: true});

	const only = process.argv[2];
	for (const [name, steps] of Object.entries(SCENARIOS)) {
		if (only && only !== name) continue;
		const recorded = await runScenario(steps);
		const fixture = {name, settleMs: SETTLE_MS, docs: DOCS, steps: recorded};
		fs.writeFileSync(path.join(OUT_DIR, `${name}.json`), JSON.stringify(fixture, null, '\t') + '\n');
		console.log(`recorded ${name}: ${recorded.reduce((n, s) => n + s.outputs.length, 0)} messages`);
	}
};

main().catch((e) => {
	console.error(e);
	process.exit(1);
});
