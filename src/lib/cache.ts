import Keyv from "keyv";
import KeyvFile from "keyv-file";
import * as path from "path";

// Just identifier->boolean so we can avoid hitting the db to check if something is there
export const userDidCache = new Keyv<boolean>({ namespace: "users", ttl: 1000 * 60 * 60 * 24 });
export const postUriCache = new Keyv<boolean>({ namespace: "posts", ttl: 1000 * 60 * 60 * 24 });

export const cursorPersist = new Keyv({
	namespace: "cursor",
	ttl: 1000 * 60 * 60 * 24 * 3,
	store: new KeyvFile({ filename: path.join(__dirname, "..", "cursor.json"), writeDelay: 1000 * 15 }),
});
export const failedMessages = new Keyv({
	namespace: "failed",
	store: new KeyvFile({ filename: path.join(__dirname, "..", "_failed_messages.json") }),
});
