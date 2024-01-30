export const filterExists = <T>(arr: Array<T>): Array<NonNullable<T>> =>
	arr.filter((item): item is NonNullable<T> => item != null);
