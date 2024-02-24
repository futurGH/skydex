import { AtpBaseClient } from "@atproto/api";
import { cborToLexRecord, readCar } from "@atproto/repo";
import { Frame } from "@atproto/xrpc-server";
import * as util from "util";
import { parseArgs } from "util";
import { type RawData, WebSocket } from "ws";
import * as AppBskyActorProfile from "../lexicons/types/app/bsky/actor/profile.ts";
import * as AppBskyFeedLike from "../lexicons/types/app/bsky/feed/like.ts";
import * as AppBskyFeedPost from "../lexicons/types/app/bsky/feed/post.ts";
import * as AppBskyFeedRepost from "../lexicons/types/app/bsky/feed/repost.ts";
import * as AppBskyGraphFollow from "../lexicons/types/app/bsky/graph/follow.ts";
import * as ComAtprotoSyncSubscribeRepos from "../lexicons/types/com/atproto/sync/subscribeRepos.ts";
import { BOTTLENECK_OPTIONS, rateLimiter } from "./lib/api.ts";
import { cursorPersist, failedMessages } from "./lib/cache.ts";
import {
	handleActorCreate,
	handleActorDelete,
	handleActorUpdate,
	handleFollowCreate,
	handleFollowDelete,
	handleHandleUpdate,
	handleLikeCreate,
	handleLikeDelete,
	handlePostCreate,
	handlePostDelete,
	handleRepostCreate,
	handleRepostDelete,
} from "./lib/handleRepoOperation.ts";

const { values: args } = parseArgs({
	args: process.argv.slice(2),
	options: { verbose: { type: "boolean", default: false } },
});

const atpClient = new AtpBaseClient();

let cursor: unknown = await cursorPersist.get("cursor");

async function handleFirehoseMessage(data: RawData) {
	const frame = Frame.fromBytes(data as never);

	if (frame.isError()) throw frame.body;
	if (!frame.header.t || !frame.body || typeof frame.body !== "object") {
		throw new Error("Invalid frame structure: " + util.inspect(frame, false, 2));
	}

	const message = atpClient.xrpc.lex.assertValidXrpcMessage("com.atproto.sync.subscribeRepos", {
		$type: `com.atproto.sync.subscribeRepos${frame.header.t}`,
		...frame.body,
	});

	if (ComAtprotoSyncSubscribeRepos.isHandle(message)) {
		try {
			await handleHandleUpdate({ repo: message.did, handle: message.handle });
		} catch (e) {
			await failedMessages.set(`${message.did}::handle`, message);
			throw e;
		}
		cursor = message.seq;
	} else if (ComAtprotoSyncSubscribeRepos.isTombstone(message)) {
		try {
			await handleActorDelete({ repo: message.did });
		} catch (e) {
			await failedMessages.set(`${message.did}::tombstone`, message);
			throw e;
		}
		cursor = message.seq;
	} else if (ComAtprotoSyncSubscribeRepos.isIdentity(message)) {
		try {
			await handleActorUpdate({ record: {}, repo: message.did });
		} catch (e) {
			await failedMessages.set(`${message.did}::identity`, message);
			throw e;
		}
		cursor = message.seq;
	} else if (ComAtprotoSyncSubscribeRepos.isInfo(message)) {
		console.log(`ℹ️ Firehose info ${message.name}: ${message.message}`);
	} else if (ComAtprotoSyncSubscribeRepos.isCommit(message)) {
		try {
			if (!message.blocks?.length) return;

			const car = await readCar(message.blocks);
			for (const op of message.ops) {
				const uri = `at://${message.repo}/${op.path}`;
				if (op.action === "create") {
					if (!op.cid) continue;
					const rec = car.blocks.get(op.cid);
					if (!rec) continue;
					const record = cborToLexRecord(rec);

					if (AppBskyFeedPost.isRecord(record)) {
						await handlePostCreate({ record, cid: op.cid.toString(), repo: message.repo, uri });
					} else if (AppBskyFeedRepost.isRecord(record)) {
						await handleRepostCreate({ record, repo: message.repo, uri });
					} else if (AppBskyFeedLike.isRecord(record)) {
						await handleLikeCreate({ record, repo: message.repo, uri });
					} else if (AppBskyGraphFollow.isRecord(record)) {
						await handleFollowCreate({ record, repo: message.repo, uri });
					} else if (AppBskyActorProfile.isRecord(record)) {
						await handleActorCreate({ repo: message.repo });
					}
				} else if (op.action === "update") {
					if (!op.cid) continue;
					const rec = car.blocks.get(op.cid);
					if (!rec) continue;
					const record = cborToLexRecord(rec);

					if (AppBskyActorProfile.isRecord(record)) {
						await handleActorUpdate({ record, repo: message.repo });
					}
				} else if (op.action === "delete") {
					const rkey = op.path?.split("/")?.pop();
					if (!rkey) continue;

					if (op.path.startsWith("app.bsky.feed.post")) {
						await handlePostDelete({ uri });
					} else if (op.path.startsWith("app.bsky.feed.repost")) {
						await handleRepostDelete({ repo: message.repo, rkey });
					} else if (op.path.startsWith("app.bsky.feed.like")) {
						await handleLikeDelete({ repo: message.repo, rkey });
					} else if (op.path.startsWith("app.bsky.graph.follow")) {
						await handleFollowDelete({ repo: message.repo, rkey });
					}
				}
			}
		} catch (e) {
			await failedMessages.set(`${message.repo}::${message.rev}`, { message, retries: 0 });
			throw e;
		}

		cursor = message.seq;
	}
}

let eventCounter = 0;
let eventsPerSecond = 0;
let lastSecond = performance.now();

async function main() {
	for (const key of failedMessages.opts.store.keys()) {
		const { message, retries } = (await failedMessages.get(key)) ?? {};
		if (!message) continue;

		try {
			await handleFirehoseMessage(message);
			await failedMessages.delete(key);
		} catch (e) {
			if (retries && retries >= 3) {
				console.error(`🚫 Giving up on message ${key} after 3 retries`);
				await failedMessages.delete(key);
			} else {
				console.warn(`⚠️ Skipping failed message retry for ${key} due to error:`, e);
				await failedMessages.set(key, { message, retries: (retries ?? 0) + 1 });
			}
		}
	}

	const socket = new WebSocket(
		`wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos${cursor ? `?cursor=${cursor}` : ""}`,
	);

	socket.on("open", () => {
		if (args.verbose) console.info("🚰 Connected to firehose");
		setInterval(() => {
			if (eventsPerSecond >= 350) {
				rateLimiter.updateSettings({ ...BOTTLENECK_OPTIONS, minTime: 750 });
			} else if (eventsPerSecond >= 280) {
				rateLimiter.updateSettings({ ...BOTTLENECK_OPTIONS, minTime: 300 });
			} else {
				rateLimiter.updateSettings(BOTTLENECK_OPTIONS);
			}
		}, 15_000);
	});
	socket.on("message", async (data) => {
		handleFirehoseMessage(data).catch((e) => {
			console.error(e);
		});

		cursorPersist.set("cursor", cursor).catch(() => console.error("Failed to persist cursor"));

		if (args.verbose) {
			eventCounter++;
			if (performance.now() - lastSecond >= 1000) {
				console.info(`📊 ${eventCounter} events per second`);
				eventsPerSecond = eventCounter;
				eventCounter = 0;
				lastSecond = performance.now();
			}
		}
	});
	socket.on("error", (e) => console.error("Websocket error:", e));
	socket.on("close", (code, reason) => {
		console.error(`Websocket closed with code ${code}\nReason: ${reason}`);
	});
}

main().catch(console.error);
