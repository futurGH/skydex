import { AtpBaseClient } from "@atproto/api";
import { cborToLexRecord, readCar } from "@atproto/repo";
import { Frame } from "@atproto/xrpc-server";
import * as util from "util";
import { type RawData, WebSocket } from "ws";
import * as AppBskyActorProfile from "../lexicons/types/app/bsky/actor/profile.ts";
import * as AppBskyFeedLike from "../lexicons/types/app/bsky/feed/like.ts";
import * as AppBskyFeedPost from "../lexicons/types/app/bsky/feed/post.ts";
import * as AppBskyGraphFollow from "../lexicons/types/app/bsky/graph/follow.ts";
import * as ComAtprotoSyncSubscribeRepos from "../lexicons/types/com/atproto/sync/subscribeRepos.ts";

type HandleCreateParams<T> = { record: T; cid: string; repo: string; uri: string };
type HandleDeleteParams = { repo: string; uri: string };

const atpClient = new AtpBaseClient();

function handlePostCreate({ record, cid, repo, uri }: HandleCreateParams<AppBskyFeedPost.Record>) {
	// console.log("post", record);
}
function handleLikeCreate({ record, cid, repo, uri }: HandleCreateParams<AppBskyFeedLike.Record>) {
	// console.log("like", record);
}
function handleFollowCreate(
	{ record, cid, repo, uri }: HandleCreateParams<AppBskyGraphFollow.Record>,
) {
	// console.log("follow.create", record);
}
function handleActorCreate(
	{ record, cid, repo, uri }: HandleCreateParams<AppBskyActorProfile.Record>,
) {
	// console.log("actor.create", record);
}
function handlePostDelete({ repo, uri }: HandleDeleteParams) {
	// console.log("post.delete", uri);
}
function handleLikeDelete({ repo, uri }: HandleDeleteParams) {
	// console.log("like.delete", uri);
}
function handleFollowDelete({ repo, uri }: HandleDeleteParams) {
	// console.log("follow.delete", uri);
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
				handlePostCreate({ record, cid: op.cid.toString(), repo: message.repo, uri });
			} else if (AppBskyFeedLike.isRecord(record)) {
				handleLikeCreate({ record, cid: op.cid.toString(), repo: message.repo, uri });
			} else if (AppBskyGraphFollow.isRecord(record)) {
				handleFollowCreate({ record, cid: op.cid.toString(), repo: message.repo, uri });
			} else if (AppBskyActorProfile.isRecord(record)) {
				handleActorCreate({ record, cid: op.cid.toString(), repo: message.repo, uri });
			}
		} else if (op.action === "delete") {
			if (op.path.startsWith("app.bsky.feed.post")) {
				handlePostDelete({ repo: message.repo, uri });
			} else if (op.path.startsWith("app.bsky.feed.like")) {
				handleLikeDelete({ repo: message.repo, uri });
			} else if (op.path.startsWith("app.bsky.graph.follow")) {
				handleFollowDelete({ repo: message.repo, uri });
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
