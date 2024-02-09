import { createClient, EdgeDBError } from "edgedb";
import util from "util";
import e from "../dbschema/edgeql-js/index.ts";
import * as AppBskyActorProfile from "../lexicons/types/app/bsky/actor/profile.ts";
import * as AppBskyEmbedExternal from "../lexicons/types/app/bsky/embed/external.ts";
import * as AppBskyEmbedImages from "../lexicons/types/app/bsky/embed/images.ts";
import * as AppBskyEmbedRecord from "../lexicons/types/app/bsky/embed/record.ts";
import * as AppBskyEmbedRecordWithMedia from "../lexicons/types/app/bsky/embed/recordWithMedia.ts";
import type { PostView } from "../lexicons/types/app/bsky/feed/defs.ts";
import * as AppBskyFeedLike from "../lexicons/types/app/bsky/feed/like.ts";
import * as AppBskyFeedPost from "../lexicons/types/app/bsky/feed/post.ts";
import * as AppBskyFeedRepost from "../lexicons/types/app/bsky/feed/repost.ts";
import * as AppBskyGraphFollow from "../lexicons/types/app/bsky/graph/follow.ts";
import * as ComAtprotoLabelDefs from "../lexicons/types/com/atproto/label/defs.ts";
import { getPost, getProfile } from "./api.ts";
import { postUriCache, userDidCache } from "./cache.ts";
import { filterTruthy, normalize, PostProps, Result, UserProps } from "./util.ts";

type HandleCreateParams<T> = { record: T; cid: string; repo: string; uri: string };
type HandleDeleteParams = { repo: string; rkey: string };
const dbClient = createClient().withRetryOptions({ attempts: 5 });

async function apiGetPost(uri: string): Promise<Result<PostView>> {
	try {
		const result = await getPost(uri) ?? null;
		return [result, null];
	} catch (e) {
		return [null, e];
	}
}

async function resolveUser(did: string): Promise<Result<string>> {
	const cached = await userDidCache.get(did);
	if (cached) return [did, null];

	const user = await e.select(e.User, () => ({ filter_single: { did } })).run(dbClient);
	if (user) {
		await userDidCache.set(did, true);
		return [did, null];
	}

	const profile = await getProfile(did).catch((e) =>
		e instanceof Error ? e : new Error(e?.message ? e.message : util.inspect(e))
	);
	if (profile instanceof Error && profile?.message?.includes("Profile not found")) return [null, null];
	else if (profile instanceof Error || !profile?.did) return [null, profile];

	const { displayName = profile.handle, handle, description: bio = "" } = profile;

	// { id: string } -> inserted or conflict on did
	// UserProps      -> conflict on handle
	// Error          -> miscellaneous error
	// null           -> conflict on did but failed to select conflicting object
	const inserted: { id: string } | UserProps | Error | null = await e.insert(e.User, {
		did,
		displayName: normalize(displayName),
		handle: normalize(handle),
		bio: normalize(bio),
		followers: e.cast(e.User, e.set()),
	}).unlessConflict((user) => ({ on: user.handle, else: user })).run(dbClient).catch((err) => {
		// EdgeDB only allows `unlessConflict` on any one property
		// There should never be a conflict on did in theory because we checked if a user exists with this did earlier
		// Unfortunately race conditions exist, and since we're handling firehose messages asynchronously,
		// the same user could be requested multiple times simultaneously.
		// If there's a conflict on did, the least bad solution is to just make another request to query the user
		if (!(err instanceof EdgeDBError) || !err.message.includes("did violates exclusivity constraint")) {
			throw err;
		}
		return e.select(e.User, () => ({ filter_single: { did } })).run(dbClient);
	}).catch((e) => e instanceof Error ? e : new Error(e?.message ? e.message : util.inspect(e)));
	if (!inserted || inserted instanceof Error) return [null, inserted];

	// If there's a conflict on handle, we should update both users' handles with fresh data to be safe
	if ("did" in inserted && typeof inserted.did === "string") {
		const previousHandleOwnerDid = inserted.did;
		const previousHandleOwner = await getProfile(previousHandleOwnerDid).catch(async (err) => {
			// If the user has been deleted, we can give away the handle, problem solved
			if (err?.message?.includes("Profile not found")) {
				await e.delete(e.User, () => ({ filter_single: { did: previousHandleOwnerDid } })).run(
					dbClient,
				);
			}
			// Otherwise, we'll play it safe and not update or insert this user
			return err instanceof Error ? err : new Error(err?.message ? err.message : util.inspect(err));
		});
		if (previousHandleOwner instanceof Error || !previousHandleOwner?.success) {
			return [
				null,
				new Error(
					`Failed to resolve owner ${previousHandleOwnerDid} of handle @${handle} claimed by ${did}`,
					{ cause: previousHandleOwner instanceof Error ? previousHandleOwner : undefined },
				),
			];
		}

		const updatedPreviousOwner = await e.update(
			e.User,
			() => ({
				filter_single: { did: previousHandleOwnerDid },
				set: { handle: normalize(previousHandleOwner.handle) },
			}),
		).run(dbClient).catch((e) => new Error(e));
		if (!updatedPreviousOwner || updatedPreviousOwner instanceof Error) {
			return [null, updatedPreviousOwner];
		}

		const newHandleOwner = await e.insert(e.User, {
			did,
			displayName: normalize(displayName),
			handle: normalize(handle),
			bio: normalize(bio),
			followers: e.cast(e.User, e.set()),
		}).unlessConflict((user) => ({
			on: user.did,
			else: e.update(user, () => ({ set: { handle: normalize(handle) } })),
		})).run(dbClient).catch((err) => {
			// See comments on const inserted = ... .catch(err => { ... })
			if (
				!(err instanceof EdgeDBError) || !err.message.includes("did violates exclusivity constraint")
			) {
				throw err;
			}
			return e.select(e.User, () => ({ filter_single: { did } })).run(dbClient);
		}).catch((e) => e instanceof Error ? e : new Error(e?.message ? e.message : util.inspect(e)));
		if (!newHandleOwner || newHandleOwner instanceof Error) return [null, newHandleOwner];
	}

	await userDidCache.set(did, true);
	return [did, null];
}

async function resolvePost(uri: string): Promise<Result<string>> {
	const cached = await postUriCache.get(uri);
	if (cached) return [uri, null];

	const postFromDb = await e.select(e.Post, () => ({ ...e.Post["*"], filter_single: { uri } })).run(
		dbClient,
	).catch(() => null);
	if (postFromDb) {
		await postUriCache.set(uri, true);
		return [uri, null];
	}

	const [postView, postViewError] = await apiGetPost(uri).catch((e) => [null, e] as const);

	if (postViewError) return [null, postViewError];
	if (!postView) return [null, null];

	const { cid, record, author } = postView;

	if (!AppBskyFeedPost.isRecord(record) || !author?.did) {
		return [null, new Error(`Invalid PostView:\n${util.inspect(postView)}`)];
	}

	const inserted = await insertPostRecord({ record, uri, cid, repo: author.did }).catch((err) =>
		err instanceof Error ? err : null
	);
	if (!inserted) return [null, new Error(`Failed to insert post record ${uri}`)];
	if (inserted instanceof Error) return [null, inserted];

	await postUriCache.set(uri, true);
	return [uri, null];
}

async function insertPostRecord(
	{ record, repo, uri, cid }: HandleCreateParams<AppBskyFeedPost.Record>,
): Promise<Result<PostProps>> {
	const [author, authorError] = await resolveUser(repo);
	if (authorError) {
		return [
			null,
			new Error(`👤 Failed to resolve post author\n  URI: ${uri}`, { cause: authorError ?? undefined }),
		];
	}
	if (!author) return [null, null];

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

	const [parent, parentError] = parentUri ? await resolvePost(parentUri) : [],
		[root, rootError] = rootUri
			? rootUri === parentUri ? [parent, parentError] : await resolvePost(rootUri)
			: [],
		[quoted, quotedError] = quotedUri ? await resolvePost(quotedUri) : [];

	if (parentError || rootError || quotedError) {
		return [
			null,
			new Error(
				`📜 Failed to resolve post references\n  URI: ${uri}\n  Parent: ${parentError}\n  Root: ${rootError}\n  Quoted: ${quotedError}`,
				{ cause: parentError ?? rootError ?? quotedError ?? undefined },
			),
		];
	}

	// There's another scenario where any of the 3 resolvePosts returns [null, null] because either the requested post
	// or its author have been deleted. In this case, it makes sense to pretend that post doesn't exist.

	const inserted = await e.select(
		e.insert(e.Post, {
			uri,
			cid,
			createdAt: e.datetime(new Date(record.createdAt)),

			author: e.select(e.User, () => ({ filter_single: { did: repo } })),
			text: normalize(record.text),
			embed: embed
				? e.json({
					title: embed?.title ? normalize(embed.title) : undefined,
					description: embed?.description ? normalize(embed.description) : undefined,
					uri: embed?.uri ? normalize(embed.uri) : undefined,
				})
				: undefined,
			altText,

			parent: parent ? e.select(e.Post, () => ({ filter_single: { uri: parent } })) : undefined,
			root: root ? e.select(e.Post, () => ({ filter_single: { uri: root } })) : undefined,
			quoted: quoted ? e.select(e.Post, () => ({ filter_single: { uri: quoted } })) : undefined,

			likes: e.cast(e.User, e.set()),
			reposts: e.cast(e.User, e.set()),

			langs: record.langs?.map(normalize),
			tags: record.tags?.map(normalize),
			labels: labels.map(normalize),
		}).unlessConflict((post) => ({ on: post.uri, else: post })),
		(post) => post["*"],
	).run(dbClient).catch((e) => {
		console.log("caught", e);
		return e instanceof EdgeDBError ? e : null;
	});

	if (inserted instanceof EdgeDBError) {
		console.error("edgedb error", inserted);
		return [null, new Error(`📜 Failed to insert post record\n  URI: ${uri}`, { cause: inserted })];
	}
	if (!inserted) return [null, null];

	await postUriCache.set(uri, true);
	return [inserted, null];
}

export async function handlePostCreate(
	{ record, repo, uri, cid }: HandleCreateParams<AppBskyFeedPost.Record>,
) {
	const [inserted, insertionError] = await insertPostRecord({ record, repo, uri, cid });
	if (insertionError) {
		throw new Error(`📜 Failed to insert post record\n  URI: ${uri}`, { cause: insertionError });
	}
	if (!inserted) {
		console.warn(`📜 Skipping insertion of post ${uri}`);
	}
}

export async function handleLikeCreate(
	{ record, repo, uri }: Omit<HandleCreateParams<AppBskyFeedLike.Record>, "cid">,
) {
	// Feed generators can also receive likes
	if (!record.subject.uri.includes("app.bsky.feed.post")) return;

	const [subjectPost, subjectPostError] = await resolvePost(record.subject.uri);
	if (subjectPostError) {
		throw new Error(
			`👍 Failed to resolve like subject post\n  Post URI: ${record.subject.uri}\n  Like URI: ${uri}`,
			{ cause: subjectPostError },
		);
	}
	if (!subjectPost) {
		console.warn(
			`👍 Could not resolve like subject post, skipping\n  Post URI: ${record.subject.uri}\n  Like URI: ${uri}`,
		);
		return;
	}

	const [actor, actorError] = await resolveUser(repo);
	if (actorError) {
		throw new Error(`👤 Failed to resolve like author\n  Like URI: ${uri}`, { cause: actorError });
	}
	if (!actor) {
		console.warn(`👤 Could not resolve like author, skipping\n  Like URI: ${uri}`);
		return;
	}

	const rkey = uri.split("/").pop();
	if (!rkey) throw new Error(`👍 Invalid AT URI in like create\n  URI: ${uri}`);

	const inserted = await e.update(
		e.Post,
		() => ({
			filter_single: { uri: subjectPost },
			set: {
				likes: {
					"+=": e.select(e.User, () => ({ filter_single: { did: repo }, "@rkey": e.str(rkey) })),
				},
			},
		}),
	).run(dbClient).catch((e) => e instanceof Error ? e : new Error(e?.message ? e : util.inspect(e)));
	if (inserted instanceof Error) {
		throw new Error(
			`👍 Failed to insert like record\n  Like URI: ${uri}\n  Post URI: ${record.subject.uri}`,
			{ cause: inserted },
		);
	}
}

export async function handleFollowCreate(
	{ record, repo, uri }: Omit<HandleCreateParams<AppBskyGraphFollow.Record>, "cid">,
) {
	const [subjectActor, subjectActorError] = await resolveUser(record.subject);
	if (subjectActorError) {
		throw new Error(
			`👤 Failed to resolve follow subject\n  Subject DID: ${record.subject}\n  Source DID: ${repo}`,
			{ cause: subjectActorError },
		);
	}
	if (!subjectActor) {
		console.warn(
			`👤 Could not resolve follow subject, skipping\n  Subject DID: ${record.subject}\n  Source DID: ${repo}`,
		);
		return;
	}

	const [actor, actorError] = await resolveUser(repo);
	if (actorError) {
		throw new Error(`👤 Failed to resolve follow author\n  DID: ${repo}`, { cause: actorError });
	}
	if (!actor) {
		console.warn(`👤 Could not resolve follow author, skipping\n  DID: ${repo}`);
		return;
	}

	const rkey = uri.split("/").pop();
	if (!rkey) throw new Error(`👥 Invalid AT URI in follow create\n  URI: ${uri}`);

	const inserted = await e.update(
		e.User,
		() => ({
			filter_single: { did: subjectActor },
			set: {
				followers: {
					"+=": e.select(e.User, () => ({ filter_single: { did: repo }, "@rkey": e.str(rkey) })),
				},
			},
		}),
	).run(dbClient).catch((e) => e instanceof Error ? e : new Error(e?.message ? e : util.inspect(e)));
	if (inserted instanceof Error) {
		throw new Error(
			`👥 Failed to insert follow record\n  Follow URI: ${uri}\n  Subject DID: ${record.subject}`,
			{ cause: inserted },
		);
	}
}

export async function handleActorCreate({ repo }: { repo: string }) {
	// We can't insert the user directly based on the firehose record because it's missing `handle`
	const [inserted, insertionError] = await resolveUser(repo);
	if (insertionError) {
		throw new Error(`👤 Failed to insert new actor record\n  DID: ${repo}`, { cause: insertionError });
	}
	if (!inserted) {
		console.warn(`👤 Could not insert new actor record\n  DID: ${repo}`);
	}
}

export async function handleRepostCreate(
	{ record, repo, uri }: Omit<HandleCreateParams<AppBskyFeedRepost.Record>, "cid">,
) {
	const [subjectPost, subjectPostError] = await resolvePost(record.subject.uri);
	if (subjectPostError) {
		throw new Error(
			`🔁 Failed to resolve repost subject post\n  Repost URI: ${uri}\n  Post URI: ${record.subject.uri}`,
			{ cause: subjectPostError },
		);
	}
	if (!subjectPost) {
		console.warn(
			`🔁 Could not resolve repost subject post, skipping\n  Repost URI: ${uri}\n  Post URI: ${record.subject.uri}`,
		);
		return;
	}

	const [reposter, reposterError] = await resolveUser(repo);
	if (reposterError) {
		throw new Error(
			`🔁 Failed to resolve repost author\n  Repost URI: ${uri}\n  Post URI: ${record.subject.uri}`,
			{ cause: reposterError },
		);
	}
	if (!reposter) {
		console.warn(
			`🔁 Could not resolve repost author, skipping\n  Repost URI: ${uri}\n  Post URI: ${record.subject.uri}`,
		);
		return;
	}

	const rkey = uri.split("/").pop();
	if (!rkey) throw new Error(`🔁 Invalid AT URI in repost create\n  URI: ${uri}`);

	const inserted = await e.update(
		e.Post,
		() => ({
			filter_single: { uri: subjectPost },
			set: {
				reposts: {
					"+=": e.select(e.User, () => ({ filter_single: { did: repo }, "@rkey": e.str(rkey) })),
				},
			},
		}),
	).run(dbClient).catch((e) => e instanceof Error ? e : new Error(e?.message ? e : util.inspect(e)));
	if (!inserted || inserted instanceof Error) {
		throw new Error(
			`🔁 Failed to insert repost record\n  Repost URI: ${uri}\n  Post URI: ${record.subject.uri}`,
			{ cause: inserted instanceof Error ? inserted : undefined },
		);
	}
}

export async function handleActorUpdate(
	{ record, repo }: { record: AppBskyActorProfile.Record; repo: string },
) {
	const [actor, actorError] = await resolveUser(repo);
	if (actorError) {
		throw new Error(`👤 Failed to resolve actor to update\n  DID: ${repo}`, { cause: actorError });
	}
	if (!actor) {
		console.warn(`👤 Could not resolve actor to update, skipping\n  DID: ${repo}`);
		return;
	}

	const updated = await e.params(
		{ displayName: e.optional(e.str), bio: e.optional(e.str) },
		(params) =>
			e.update(
				e.User,
				(user) => ({
					filter_single: { did: repo },
					set: {
						displayName: e.op(params.displayName, "??", user.displayName),
						bio: e.op(params.bio, "??", user.bio),
					},
				}),
			),
	).run(dbClient, {
		displayName: record.displayName ? normalize(record.displayName) : null,
		bio: record.description ? normalize(record.description) : null,
	}).catch((e) => e instanceof Error ? e : new Error(e?.message ? e : util.inspect(e)));
	if (!updated || updated instanceof Error) {
		throw new Error(`👤 Failed to update actor record\n  DID: ${repo}`, {
			cause: updated instanceof Error ? updated : undefined,
		});
	}
}

export async function handleHandleUpdate({ repo, handle }: { repo: string; handle: string }) {
	const [actor, actorError] = await resolveUser(repo);
	if (actorError) {
		throw new Error(`👤 Failed to resolve actor to update\n  DID: ${repo}`, { cause: actorError });
	}
	if (!actor) {
		console.warn(`👤 Could not resolve actor to update, skipping\n  DID: ${repo}`);
		return;
	}

	const updated = await e.update(
		e.User,
		() => ({ filter_single: { did: repo }, set: { handle: normalize(handle) } }),
	).run(dbClient).catch((e) => e instanceof Error ? e : new Error(e?.message ? e : util.inspect(e)));
	if (!updated || updated instanceof Error) {
		throw new Error(`👤 Failed to update actor record\n  DID: ${repo}`, {
			cause: updated instanceof Error ? updated : undefined,
		});
	}
}

export async function handlePostDelete({ uri }: { uri: string }) {
	try {
		await e.delete(e.Post, () => ({ filter_single: { uri } })).run(dbClient);
	} catch (e) {
		throw new Error(`📜 Failed to delete post record\n  URI: ${uri}`, { cause: e });
	} finally {
		await postUriCache.delete(uri);
	}
}

export async function handleLikeDelete({ repo, rkey }: HandleDeleteParams) {
	try {
		await e.update(
			e.Post,
			(post) => ({
				filter_single: e.op(
					e.op(post.likes.did, "=", repo),
					"and",
					e.op(post.likes["@rkey"], "=", rkey),
				),
				set: { likes: { "-=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
			}),
		).run(dbClient);
	} catch (e) {
		throw new Error(`👍 Failed to delete like record\n  URI: at://${repo}/app.bsky.feed.like/${rkey}`, {
			cause: e,
		});
	}
}

export async function handleFollowDelete({ repo, rkey }: HandleDeleteParams) {
	try {
		await e.update(
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
	} catch (e) {
		throw new Error(
			`👥 Failed to delete follow record\n  URI: at://${repo}/app.bsky.graph.follow/${rkey}`,
			{ cause: e },
		);
	}
}

export async function handleRepostDelete({ repo, rkey }: HandleDeleteParams) {
	try {
		await e.update(
			e.Post,
			(post) => ({
				filter_single: e.op(
					e.op(post.reposts.did, "=", repo),
					"and",
					e.op(post.reposts["@rkey"], "=", rkey),
				),
				set: { reposts: { "-=": e.select(e.User, () => ({ filter_single: { did: repo } })) } },
			}),
		).run(dbClient);
	} catch (e) {
		throw new Error(
			`🔁 Failed to delete repost record\n  URI: at://${repo}/app.bsky.feed.repost/${rkey}`,
			{ cause: e },
		);
	}
}

export async function handleActorDelete({ repo }: { repo: string }) {
	try {
		await e.delete(e.User, () => ({ filter_single: { did: repo } })).run(dbClient);
	} catch (e) {
		throw new Error(`👤 Failed to delete actor record\n  DID: ${repo}`, { cause: e });
	} finally {
		await userDidCache.delete(repo);
	}
}
