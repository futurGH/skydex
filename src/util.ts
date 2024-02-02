import type { helper, Post, User } from "../dbschema/interfaces.ts";

export const filterTruthy = <T>(arr: Array<T>): Array<Exclude<T, false | null | undefined>> =>
	arr.filter((item): item is Exclude<T, false | null | undefined> => !!item);

export type UserProps = helper.Props<User>;
export type PostProps = helper.Props<Post>;
