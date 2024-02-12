import type { helper, Post, User } from "../../dbschema/interfaces.ts";

export const filterTruthy = <T>(arr: Array<T>): Array<Exclude<T, false | null | undefined>> =>
	arr.filter((item): item is Exclude<T, false | null | undefined> => !!item);

// https://github.com/edgedb/edgedb/blob/2551eb84a80ab11ed8d45b4b6328878b077e06d7/edb/edgeql-parser/tests/tokenizer.rs#L737
const prohibitedCharacters = /[\u{202A}-\u{202E}|\u{2066}-\u{2069}]/gu;
export const normalize = (str: string) => str.replace(prohibitedCharacters, "");

export type Result<T, E = unknown> = [T, null] | [null, E];

export type UserProps = helper.Props<User>;
export type PostProps = helper.Props<Post>;
