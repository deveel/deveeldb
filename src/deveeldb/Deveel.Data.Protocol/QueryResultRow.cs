using System;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Protocol {
	public sealed class QueryResultRow {
		public QueryResultRow(ISqlObject[] values, int[] valueSizes) {
			if (values == null)
				throw new ArgumentNullException("values");
			if (valueSizes == null)
				throw new ArgumentNullException("valueSizes");

			if (values.Length != valueSizes.Length)
				throw new ArgumentException();

			Values = values;
			ValueSizes = valueSizes;
		}

		public ISqlObject[] Values { get; private set; }

		public int[] ValueSizes { get; private set; }
	}
}
