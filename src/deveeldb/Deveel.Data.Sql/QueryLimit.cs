using System;

namespace Deveel.Data.Sql {
	public sealed class QueryLimit {
		public QueryLimit(long total) 
			: this(-1, total) {
		}

		public QueryLimit(long offset, long count) {
			if (count < 1)
				throw new ArgumentException("The limit clause must have at least one element.");
			if (offset < 0)
				offset = 0;

			Offset = offset;
			Count = count;
		}

		public long Offset { get; private set; }

		public long Count { get; private set; }
	}
}
