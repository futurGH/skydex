import Keyv from "keyv";

// Just identifier->boolean so we can avoid hitting the db to check if something is there
export const userDidCache = new Keyv<boolean>({ namespace: "users", ttl: 1000 * 60 * 60 * 24 });
export const postUriCache = new Keyv<boolean>({ namespace: "posts", ttl: 1000 * 60 * 60 * 24 });
