export class Batcher<T> {
	private queue = new Set<string>();
	private timer: NodeJS.Timeout | null = null;
	private promises = new Map<string, { resolve: (value: T) => void; reject: (reason: unknown) => void }>();

	constructor(
		public process: (this: Batcher<T>, queue: Array<string>) => Promise<Record<string, T>>,
		public options: { maxTime: number; maxSize: number },
	) {}

	async add(id: string) {
		return new Promise<T>((resolve, reject) => {
			this.queue.add(id);
			this.promises.set(id, { resolve, reject });

			if (this.queue.size >= this.options.maxSize) {
				this.flush();
			} else {
				this.scheduleFlush();
			}
		});
	}

	private scheduleFlush(): void {
		if (!this.timer) this.timer = setTimeout(() => this.flush(), this.options.maxTime);
	}

	async flush() {
		if (this.timer) {
			clearTimeout(this.timer);
			this.timer = null;
		}

		const queue = [...this.queue];
		this.queue.clear();

		try {
			const result = await this.process(queue);
			for (const id of queue) {
				this.promises.get(id)?.resolve(result[id]);
				this.promises.delete(id);
			}
		} catch (error) {
			for (const id of queue) {
				this.promises.get(id)?.reject(error);
				this.promises.delete(id);
			}
		}
	}
}
