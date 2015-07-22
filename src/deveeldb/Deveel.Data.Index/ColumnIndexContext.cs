using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.Index {
	public sealed class ColumnIndexContext {
		internal ColumnIndexContext(ITable table, int columnOffset, IEnumerable<KeyValuePair<string, object>> metadata) {
			Table = table;
			ColumnOffset = columnOffset;
			Metadata = metadata;
		}

		public ITable Table { get; private set; }

		public string ColumnName {
			get { return Table.TableInfo[ColumnOffset].ColumnName; }
		}

		public DataType ColumnType {
			get { return Table.TableInfo[ColumnOffset].ColumnType; }
		}

		public int ColumnOffset { get; private set; }

		public IEnumerable<KeyValuePair<string, object>> Metadata { get; private set; }
	}
}
