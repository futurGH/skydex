import { AtpAgent, AtpBaseClient, AtUri } from "@atproto/api";
import { cborToLexRecord, readCar } from "@atproto/repo";
import { Frame } from "@atproto/xrpc-server";
import Bottleneck from "bottleneck";
import { createClient } from "edgedb";
import * as util from "util";
import { type RawData, WebSocket } from "ws";
import e from "../dbschema/edgeql-js";
import * as AppBskyActorProfile from "../lexicons/types/app/bsky/actor/profile.ts";
import * as AppBskyEmbedExternal from "../lexicons/types/app/bsky/embed/external.ts";
import * as AppBskyEmbedImages from "../lexicons/types/app/bsky/embed/images.ts";
import * as AppBskyEmbedRecord from "../lexicons/types/app/bsky/embed/record.ts";
import * as AppBskyEmbedRecordWithMedia from "../lexicons/types/app/bsky/embed/recordWithMedia.ts";
import * as AppBskyFeedLike from "../lexicons/types/app/bsky/feed/like.ts";
import * as AppBskyFeedPost from "../lexicons/types/app/bsky/feed/post.ts";
import * as AppBskyGraphFollow from "../lexicons/types/app/bsky/graph/follow.ts";
import * as ComAtprotoLabelDefs from "../lexicons/types/com/atproto/label/defs.ts";
import * as ComAtprotoSyncSubscribeRepos from "../lexicons/types/com/atproto/sync/subscribeRepos.ts";
import { postUriCache, userDidCache } from "./cache.ts";
import { filterTruthy } from "./util.ts";

type HandleCreateParams<T> = { record: T; cid: string; repo: string; uri: string };
type HandleDeleteParams = { repo: string; uri: string };

const atpClient = new AtpBaseClient();
const atpAgent = new AtpAgent({ service: "https://bsky.social" });
const dbClient = createClient();

// Rate limit is 3000/5min (10/s); we use slightly conservative numbers to be safe
const rateLimiter = new Bottleneck({
	minTime: 110,
	reservoir: 2800,
	reservoirRefreshAmount: 2800,
	reservoirRefreshInterval: 5 * 60 * 1000,
});

const backoffs = new Map<string, number>();
rateLimiter.on("failed", (error, jobInfo) => {
	// retryCount=0 → 250ms
	// retryCount=1 → 707ms
	// retryCount=2 → 3 674ms
	// retryCount=3 → 29 393ms
	// retryCount=4 → 328 633ms
	// retryCount>4 → give up
	let backoff = backoffs.get(jobInfo.options.id) ?? (backoffs.set(jobInfo.options.id, 250), 250);
	if (error?.statusCode === 429) {
		// Wait until rate limit resets if we have 0 requests remaining
		if (error?.headers?.["ratelimit-remaining"] && error?.headers?.["ratelimit-reset"]) {
			if (error.headers["ratelimit-remaining"] === "0") {
				const reset = parseInt(error.headers["ratelimit-reset"]) * 1000;
				const wait = reset - Date.now();
				backoffs.set(jobInfo.options.id, wait);
				return wait;
			}
		}
		if (jobInfo.retryCount < 5) {
			backoffs.set(jobInfo.options.id, backoff = backoff * (jobInfo.retryCount + 1) ** 1.5);
			return backoff;
		} else {
			backoffs.delete(jobInfo.options.id);
			console.error(
				`🚫 Giving up after 5 retries\n  ID: ${jobInfo.options.id}\n  Error: ${util.inspect(error)}`,
			);
			return null;
		}
	} else {
		console.error(
			`❗ Skipping invalid request\n  ID: ${jobInfo.options.id}\n  Error: ${util.inspect(error)}`,
		);
		return null;
	}
});

async function resolveUser(did: string): Promise<string | null> {
	const cached = await userDidCache.get(did);
	if (cached) return did;

	const user = await e.select(e.User, () => ({ ...e.User["*"], filter_single: { did } })).run(dbClient);
	if (user) {
		await userDidCache.set(did, true);
		return did;
	}

	const profile = await rateLimiter.schedule(
		{ id: `app.bsky.actor.getProfile::${did}` },
		() => atpAgent.api.app.bsky.actor.getProfile({ actor: did }),
	);
	if (!profile?.success) return null;
	const { displayName = profile.data.handle, handle, description: bio = "" } = profile.data;

	const inserted = await e.insert(e.User, {
		did,
		displayName,
		handle,
		bio,
		followers: e.cast(e.User, e.set()),
	}).unlessConflict((user) => ({
		on: user.did,
		else: e.select(e.User, () => ({ ...e.User["*"], filter_single: { did } })),
	})).run(dbClient).catch(() => null);
	if (!inserted) return null;

	await userDidCache.set(did, true);
	return did;
}

async function resolvePost(uri: string): Promise<string | null> {
	const cached = await postUriCache.get(uri);
	if (cached) return uri;

	const postFromDb = await e.select(e.Post, () => ({ ...e.Post["*"], filter_single: { uri } })).run(
		dbClient,
	);
	if (postFromDb) {
		await postUriCache.set(uri, true);
		return uri;
	}

	const { host: repo, rkey } = new AtUri(uri);
	const { cid, value: record } = await rateLimiter.schedule(
		{ id: `app.bsky.feed.post.get::${uri}` },
		() => atpAgent.api.app.bsky.feed.post.get({ repo, rkey }),
	).catch(() => ({ cid: null, value: null }));
	if (!record) return null;

	const inserted = await insertPostRecord({ record, repo, uri, cid }).catch(() => null);
	if (!inserted) return null;

	await postUriCache.set(uri, true);
	return uri;
}

async function insertPostRecord({ record, repo, uri, cid }: HandleCreateParams<AppBskyFeedPost.Record>) {
	const author = await resolveUser(repo);
	if (!author) throw new Error(`👤 Failed to resolve post author\n  URI: ${uri}`);

	const labels = ComAtprotoLabelDefs.isSelfLabels(record.labels)
		? record.labels.values.map(({ val }) => val)
		: [];

	let altText: string | undefined;
	let embed: AppBskyEmbedExternal.External | undefined;
	let quotedUri: string | undefined;

	if (AppBskyEmbedImages.isMain(record.embed)) {
		altText = filterTruthy(record.embed.images.map((i) => i.alt)).join("\n");
	} else if (AppBskyEmbedExternal.isMain(record.embed)) {
		embed = record.embed.external;
		// No point in inserting an empty embed
		if (!embed.title && !embed.description && !embed.uri) embed = undefined;
	} else if (AppBskyEmbedRecord.isMain(record.embed)) {
		quotedUri = record.embed.record.uri;
	} else if (AppBskyEmbedRecordWithMedia.isMain(record.embed)) {
		quotedUri = record.embed.record.record.uri;
	}

	const parentUri = record.reply?.parent?.uri;
	const rootUri = record.reply?.root?.uri;

	const parent = parentUri ? await resolvePost(parentUri) : undefined,
		root = rootUri ? await resolvePost(rootUri) : undefined,
		quoted = quotedUri ? await resolvePost(quotedUri) : undefined;

	const inserted = await e.select(
		e.insert(e.Post, {
			uri,
			cid,
			createdAt: e.datetime(new Date(record.createdAt)),

			author: e.select(e.User, () => ({ filter_single: { id: repo } })),
			text: record.text,
			embed: embed
				? e.insert(e.Embed, { title: embed?.title, description: embed?.description, uri: embed?.uri })
				: undefined,
			altText,

			parent: parent ? e.select(e.Post, () => ({ filter_single: { uri: parent } })) : undefined,
			root: root ? e.select(e.Post, () => ({ filter_single: { uri: root } })) : undefined,
			quoted: quoted ? e.select(e.Post, () => ({ filter_single: { uri: quoted } })) : undefined,

			likes: e.cast(e.User, e.set()),
			reposts: e.cast(e.User, e.set()),

			langs: record.langs,
			tags: record.tags,
			labels,
		}).unlessConflict((post) => ({
			on: post.uri,
			else: e.select(e.Post, () => ({ ...e.Post["*"], filter_single: { uri } })),
		})),
		(post) => post["*"],
	).run(dbClient);

	if (!inserted) throw new Error(`📜 Failed to insert post record\n  URI: ${uri}`);
	await postUriCache.set(uri, true);
	return inserted;
}

async function handleLikeCreate(
	{ record, repo, uri }: Omit<HandleCreateParams<AppBskyFeedLike.Record>, "cid">,
) {
	const subjectPost = await resolvePost(record.subject.uri);
	if (!subjectPost) {
		throw new Error(
			`👍 Failed to resolve like subject post\n  Post URI: ${record.subject.uri}\n  Like URI: ${uri}`,
		);
	}

	const actor = await resolveUser(repo);
	if (!actor) throw new Error(`👤 Failed to resolve like author\n  Like URI: ${uri}`);

	const inserted = await e.update(
		e.Post,
		() => ({
			filter_single: { uri: subjectPost },
			set: { likes: { "+=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
		}),
	).run(dbClient);
	if (!inserted) {
		throw new Error(
			`👍 Failed to insert like record\n  Like URI: ${uri}\n  Post URI: ${record.subject.uri}`,
		);
	}
}

async function handleFollowCreate(
	{ record, repo, uri }: Omit<HandleCreateParams<AppBskyGraphFollow.Record>, "cid">,
) {
	const subjectActor = await resolveUser(record.subject);
	if (!subjectActor) {
		throw new Error(
			`👤 Failed to resolve follow subject\n  Subject DID: ${record.subject}\n  Source DID: ${repo}`,
		);
	}

	const actor = await resolveUser(repo);
	if (!actor) throw new Error(`👤 Failed to resolve follow author\n  DID: ${repo}`);

	const inserted = await e.update(
		e.User,
		() => ({
			filter_single: { did: subjectActor },
			set: { followers: { "+=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
		}),
	).run(dbClient);
	if (!inserted) {
		throw new Error(
			`👥 Failed to insert follow record\n  Follow URI: ${uri}\n  Subject DID: ${record.subject}`,
		);
	}
}

async function handleActorCreate({ repo }: { repo: string }) {
	// We can't insert the user directly based on the firehose record because it's missing `handle`
	const inserted = await resolveUser(repo);
	if (!inserted) {
		throw new Error(`👤 Failed to insert new actor record\n  DID: ${repo}`);
	}
}

async function handlePostDelete({ uri }: { uri: string }) {
	const removed = await e.delete(e.Post, () => ({ filter_single: { uri } })).run(dbClient);
	if (!removed) {
		throw new Error(`📜 Failed to delete post record\n  URI: ${uri}`);
	}
}
async function handleLikeDelete({ repo, uri }: HandleDeleteParams) {
	const rkey = uri.split("/").pop();
	if (!rkey) throw new Error(`👍 Invalid AT URI in like delete\n  URI: ${uri}`);
	const updated = await e.update(
		e.Post,
		(post) => ({
			filter_single: e.op(e.op(post.likes.did, "=", repo), "and", e.op(post.likes["@rkey"], "=", rkey)),
			set: { likes: { "-=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
		}),
	).run(dbClient);
	if (!updated) {
		throw new Error(`👍 Failed to delete like record\n  URI: ${uri}`);
	}
}
async function handleFollowDelete({ repo, uri }: HandleDeleteParams) {
	const rkey = uri.split("/").pop();
	if (!rkey) throw new Error(`👥 Invalid AT URI in follow delete\n  URI: ${uri}`);
	const updated = await e.update(
		e.User,
		(user) => ({
			filter_single: e.op(
				e.op(user.followers.did, "=", repo),
				"and",
				e.op(user.followers["@rkey"], "=", rkey),
			),
			set: { followers: { "-=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
		}),
	).run(dbClient);
	if (!updated) {
		throw new Error(`👥 Failed to delete follow record\n  URI: ${uri}`);
	}
}

async function handleMessage(data: RawData) {
	const frame = Frame.fromBytes(data as never);

	if (frame.isError()) throw frame.body;
	if (!frame.header.t || !frame.body || typeof frame.body !== "object") {
		throw new Error("Invalid frame structure: " + util.inspect(frame, false, 2));
	}

	const message = atpClient.xrpc.lex.assertValidXrpcMessage("com.atproto.sync.subscribeRepos", {
		$type: `com.atproto.sync.subscribeRepos${frame.header.t}`,
		...frame.body,
	});

	// todo: handle handles
	if (!ComAtprotoSyncSubscribeRepos.isCommit(message)) return;
	if (!message.blocks?.length) return;

	const car = await readCar(message.blocks);
	for (const op of message.ops) {
		const uri = `at://${message.repo}/${op.path}`;
		if (op.action === "create" || op.action === "update") {
			if (!op.cid) continue;
			const rec = car.blocks.get(op.cid);
			if (!rec) continue;
			const record = cborToLexRecord(rec);

			if (AppBskyFeedPost.isRecord(record)) {
				await insertPostRecord({ record, cid: op.cid.toString(), repo: message.repo, uri });
			} else if (AppBskyFeedLike.isRecord(record)) {
				await handleLikeCreate({ record, repo: message.repo, uri });
			} else if (AppBskyGraphFollow.isRecord(record)) {
				await handleFollowCreate({ record, repo: message.repo, uri });
			} else if (AppBskyActorProfile.isRecord(record)) {
				await handleActorCreate({ repo: message.repo });
			}
		} else if (op.action === "delete") {
			if (op.path.startsWith("app.bsky.feed.post")) {
				await handlePostDelete({ uri });
			} else if (op.path.startsWith("app.bsky.feed.like")) {
				await handleLikeDelete({ repo: message.repo, uri });
			} else if (op.path.startsWith("app.bsky.graph.follow")) {
				await handleFollowDelete({ repo: message.repo, uri });
			}
		}
	}
}

async function main() {
	const socket = new WebSocket("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos");
	socket.on("message", async (data) => {
		handleMessage(data).catch(console.error);
	});
	socket.on("error", (e) => console.error("Websocket error:", e));
}

main().catch(console.error);
