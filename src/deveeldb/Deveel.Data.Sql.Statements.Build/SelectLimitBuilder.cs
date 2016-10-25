using System;

namespace Deveel.Data.Sql.Statements.Build {
	class SelectLimitBuilder : ISelectLimitBuilder {
		private long? count;
		private long? offset;

		public ISelectLimitBuilder Count(long value) {
			count = value;
			return this;
		}

		public ISelectLimitBuilder Offset(long value) {
			offset = value;
			return this;
		}

		public QueryLimit Build() {
			if (count == null)
				throw new InvalidOperationException("The limit count is required");

			if (offset != null)
				return new QueryLimit(offset.Value, count.Value);

			return new QueryLimit(count.Value);
		}
	}
}
