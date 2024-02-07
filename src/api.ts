import { AtpAgent } from "@atproto/api";
import Bottleneck from "bottleneck";
import util from "util";
import type { ProfileViewDetailed } from "../lexicons/types/app/bsky/actor/defs.ts";
import { PostView } from "../lexicons/types/app/bsky/feed/defs.ts";
import { Batcher } from "./util/batch.ts";

const atpAgent = new AtpAgent({ service: "https://api.bsky.app" });

// https://www.docs.bsky.app/docs/api/app-bsky-feed-get-posts#request
const MAX_BATCH_SIZE = 25;

// Rate limit is 3000/5min (1q/100ms); we use slightly conservative numbers to be safe
export const BOTTLENECK_OPTIONS: Bottleneck.ConstructorOptions = {
	minTime: 110,
	reservoir: 2900,
	reservoirRefreshAmount: 2900,
	reservoirRefreshInterval: 5 * 60 * 1000,
};
export const rateLimiter = new Bottleneck(BOTTLENECK_OPTIONS);

const backoffs = new Map<string, number>();
rateLimiter.on("failed", (error, jobInfo) => {
	// retryCount=0 → 250ms
	// retryCount=1 → 707ms
	// retryCount=2 → 3 674ms
	// retryCount=3 → 29 393ms
	// retryCount=4 → 328 633ms
	// retryCount>4 → give up

	const statusCode = error?.statusCode ?? error?.status;

	let backoff = backoffs.get(jobInfo.options.id) ?? (backoffs.set(jobInfo.options.id, 250), 250);
	if (statusCode === 429) {
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
rateLimiter.on("error", (error) => {
	console.error("🚨 Rate limiter error:", error);
});

// Avoid duplicating queries by caching in-progress requests by ID
const unresolvedPromises = new Map<string, Promise<unknown>>();
async function limit<T>(options: Bottleneck.JobOptions, fn: () => Promise<T>): Promise<T> {
	const id = options.id ?? `unknown::${Date.now()}`;
	const existing = unresolvedPromises.get(id);
	if (existing) return existing as Promise<T>;

	const scheduled = rateLimiter.schedule(options, fn).then((res) => {
		unresolvedPromises.delete(id);
		return res;
	});
	unresolvedPromises.set(id, scheduled);
	return scheduled;
}

const getProfilesBatch = new Batcher<ProfileViewDetailed>(async function(actors) {
	const result = await limit(
		{ id: `getProfilesBatch::${actors.length}::${Date.now()}` },
		() => atpAgent.api.app.bsky.actor.getProfiles({ actors }),
	);
	if (!result.success) throw result;
	return result.data.profiles.reduce<Record<string, ProfileViewDetailed>>((acc, profile) => {
		acc[profile.did] = profile;
		return acc;
	}, {});
}, { maxTime: 1000, maxSize: MAX_BATCH_SIZE });

const getPostsBatch = new Batcher<PostView>(async function(uris) {
	const result = await limit(
		{ id: `getPostsBatch::${uris.length}::${Date.now()}` },
		() => atpAgent.api.app.bsky.feed.getPosts({ uris }),
	);
	if (!result.success) throw result;
	return result.data.posts.reduce<Record<string, PostView>>((acc, post) => {
		acc[post.uri] = post;
		return acc;
	}, {});
}, { maxTime: 1000, maxSize: MAX_BATCH_SIZE });

export const getProfile = async (did: string) => getProfilesBatch.add(did);
export const getPost = (uri: string) => getPostsBatch.add(uri);
