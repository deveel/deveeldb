using System;
using System.IO;

namespace Deveel.Data.Client {
	internal class QueryResponse {
		public QueryResponse(BinaryReader input) {
			resultId = input.ReadInt32();
			queryTime = input.ReadInt32();
			rowCount = input.ReadInt32();
			int colCount = input.ReadInt32();
			cols = new ColumnInfo[colCount];
			for (int i = 0; i < colCount; i++) {
				cols[i] = ReadColumnInfo(input);
			}
		}

		private readonly int resultId;
		private readonly int queryTime;
		private readonly int rowCount;
		private readonly ColumnInfo[] cols;

		public int RowCount {
			get { return rowCount; }
		}

		public int QueryTime {
			get { return queryTime; }
		}

		public int ResultId {
			get { return resultId; }
		}

		public int ColumnCount {
			get { return cols.Length; }
		}

		private static ColumnInfo ReadColumnInfo(BinaryReader input) {
			string name = input.ReadString();
			int type = input.ReadInt32();
			int size = input.ReadInt32();
			bool notNull = input.ReadBoolean();
			bool unique = input.ReadBoolean();
			int uniqueGroup = input.ReadInt32();
			SqlType sqlType = (SqlType) input.ReadInt32();
			int scale = input.ReadInt32();

			ColumnInfo columnInfo = new ColumnInfo(name, type, sqlType, size, scale, notNull);
			if (unique)
				columnInfo.SetUnique(uniqueGroup);

			return columnInfo;
		}

		public ColumnInfo GetColumn(int index) {
			return cols[index];
		}
	}
}