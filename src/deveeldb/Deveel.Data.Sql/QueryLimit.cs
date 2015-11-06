using System;

namespace Deveel.Data.Sql {
	public sealed class QueryLimit {
		public QueryLimit(long total) 
			: this(-1, total) {
		}

		public QueryLimit(long offset, long total) {
			if (total <= 1)
				throw new ArgumentException("The limit clause must have at least one element.");
			if (offset < 0)
				offset = -1;

			Offset = offset;
			Total = total;
		}

		public long Offset { get; private set; }

		public long Total { get; private set; }

		public bool HasOffset {
			get { return Offset >= 0; }
		}
	}
}
