import { AtpAgent, AtUri } from "@atproto/api";
import { RepoRecord } from "@atproto/lexicon";
import { verifyRepoCar } from "@atproto/repo";
import ansiColors from "ansi-colors";
import { MultiBar } from "cli-progress";
import * as AppBskyActorProfile from "../lexicons/types/app/bsky/actor/profile.ts";
import * as AppBskyFeedGenerator from "../lexicons/types/app/bsky/feed/generator.ts";
import * as AppBskyFeedLike from "../lexicons/types/app/bsky/feed/like.ts";
import * as AppBskyFeedPost from "../lexicons/types/app/bsky/feed/post.ts";
import * as AppBskyFeedRepost from "../lexicons/types/app/bsky/feed/repost.ts";
import * as AppBskyGraphBlock from "../lexicons/types/app/bsky/graph/block.ts";
import * as AppBskyGraphFollow from "../lexicons/types/app/bsky/graph/follow.ts";
import type { Repo as ListReposRepo } from "../lexicons/types/com/atproto/sync/listRepos.ts";
import { limit } from "./api.ts";
import {
	handleActorCreate,
	handleFollowCreate,
	handleLikeCreate,
	handlePostCreate,
	handleRepostCreate,
} from "./handleRepoOperation.ts";

const MEGABYTE = 1024 * 1024;

const relayAgent = new AtpAgent({ service: "https://bsky.network" });

const logger = new MultiBar({
	format: ({ barsize = 50 }, { total, value: _value, progress }, payload) => {
		const value = _value.toString().padStart(total.toString().length, " ");
		const percentage = Math.floor(progress * 100).toString().padStart(3, " ");
		const did = ansiColors.cyanBright(payload.did);
		const repoSize = ansiColors.grey(`(${(payload.bytes / MEGABYTE).toFixed(1)}MB)`);

		const completeBarSize = Math.round(progress * barsize);
		const barComplete = ansiColors.green("█".repeat(completeBarSize));
		const barIncomplete = ansiColors.grey("░".repeat(barsize - completeBarSize));

		return `${did} ${repoSize} ${barComplete}${barIncomplete} ${percentage}% | ${value}/${total}`;
	},
	barsize: Math.min(100, Math.max(50, process.stdout.columns - 65)),
	clearOnComplete: true,
	stopOnComplete: true,
	hideCursor: true,
	autopadding: true,
	linewrap: true,
});

async function processRecord(
	{ record, repo, uri, cid }: { record: RepoRecord; repo: string; uri: string; cid: string },
) {
	if (AppBskyFeedPost.isRecord(record)) await handlePostCreate({ record, repo, uri, cid });
	else if (AppBskyFeedLike.isRecord(record)) await handleLikeCreate({ record, repo, uri });
	else if (AppBskyFeedRepost.isRecord(record)) await handleRepostCreate({ record, repo, uri });
	else if (AppBskyGraphFollow.isRecord(record)) await handleFollowCreate({ record, repo, uri });
	else if (AppBskyActorProfile.isRecord(record)) await handleActorCreate({ repo });
	else if (AppBskyFeedGenerator.isRecord(record)) return;
	else if (AppBskyGraphBlock.isRecord(record)) return;
	else console.error(`Unknown record at ${uri}`);
}

async function processRepo({ did }: ListReposRepo) {
	const res = await limit(
		{ id: `com.atproto.sync.getRepo::${did}` },
		() => relayAgent.com.atproto.sync.getRepo({ did }),
	);
	if (!res.success) {
		console.error(`Failed to fetch repo ${did}`, res);
		return;
	}

	const repo = await verifyRepoCar(res.data, did);
	if (!repo) {
		console.error(`Failed to verify repo ${did}`);
		return;
	}

	const progressBar = logger.create(repo.creates.length, 0, { did, bytes: res.data.byteLength });

	for (const { record, collection, rkey, cid } of repo.creates) {
		const uri = AtUri.make(did, collection, rkey).toString();
		await processRecord({ record, repo: did, uri, cid: cid.toString() }).catch(console.error);
		progressBar.increment();
	}

	progressBar.stop();
}

async function backfill() {
	console.log = (...data: unknown[]) => logger.log(data.join(" ") + "\n");
	console.info = console.log;
	console.warn = (...data: unknown[]) => logger.log(ansiColors.yellow(data.join(" ") + "\n"));
	console.error = (...data: unknown[]) => logger.log(ansiColors.red(data.join(" ") + "\n"));

	let cursor: string = "";
	while (true) {
		const res = await limit({ id: `com.atproto.sync.listRepos::${cursor || 0}` }, () =>
			fetch( // Uses fetch to avoid lexicon validation that could cause us to skip 1000 repos for 1 bad one
				`https://bsky.network/xrpc/com.atproto.sync.listRepos${cursor ? `?cursor=${cursor}` : ""}`,
			));

		if (!res.ok) {
			console.error(`Failed to fetch repos, cursor=${cursor}`, res);
			break;
		}

		const data = await res.json();

		if (!data || typeof data !== "object" || !("repos" in data) || !Array.isArray(data.repos)) {
			console.error(`Invalid response for repos, cursor=${cursor}`, data);
			continue;
		}

		const { repos } = data as { repos: Array<unknown> };
		if (!repos?.length || !("cursor" in data) || typeof data.cursor !== "string") {
			console.log("No more repos to fetch");
			break;
		}
		cursor = data.cursor;

		for (const repo of repos) {
			if (typeof repo !== "object" || !repo || !["did", "head", "rev"].every((k) => k in repo)) {
				console.error(`Invalid repo object`, repo);
				continue;
			}
			await processRepo(repo as ListReposRepo).catch(console.error);
		}
	}
}

backfill().catch(console.error);
