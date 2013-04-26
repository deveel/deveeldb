// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

using Deveel.Data.Deveel.Data;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="GTDataSource"/> that maps a 11-bit <see cref="Privileges"/> 
	/// to strings that represent the privilege in human readable string.
	/// </summary>
	/// <remarks>
	/// Each 11-bit priv set contains 12 entries for each bit that was set.
	/// <para>
	/// This table provides a convenient way to join the system grant table and
	/// <i>expand</i> the privileges that are allowed though it.
	/// </para>
	/// </remarks>
	internal class GTPrivMapDataSource : GTDataSource {
		/// <summary>
		/// Number of bits.
		/// </summary>
		private const int BitCount = Privileges.BitCount;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		public static readonly DataTableInfo DataTableInfo;

		static GTPrivMapDataSource() {
			DataTableInfo info = new DataTableInfo(SystemSchema.Privileges);

			// Add column definitions
			info.AddColumn("priv_bit", TType.NumericType);
			info.AddColumn("description", TType.StringType);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;

		}

		public GTPrivMapDataSource(DatabaseConnection connection)
			: base(connection.System) {
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return (1 << BitCount)*BitCount; }
		}

		public override TObject GetCell(int column, int row) {
			int c1 = row / BitCount;
			if (column == 0)
				return GetColumnValue(column, (BigNumber) c1);

			int privBit = (1 << (row % BitCount));
			string privString = null;
			if ((c1 & privBit) != 0) {
				privString = Privileges.FormatPriv(privBit);
			}
			return GetColumnValue(column, privString);
		}


		public override SelectableScheme GetColumnScheme(int column) {
			if (column == 0)
				return new PrivMapSearch(this, column);
			return new BlindSearch(this, column);
		}

		// ---------- Inner classes ----------

		/// <summary>
		/// A SelectableScheme that makes searching on the 'priv_bit' column 
		/// a lot less painless!
		/// </summary>
		private sealed class PrivMapSearch : CollatedBaseSearch {

			internal PrivMapSearch(ITableDataSource table, int column)
				: base(table, column) {
			}

			public override SelectableScheme Copy(ITableDataSource table, bool immutable) {
				// Return a fresh object.  This implementation has no state so we can
				// ignore the 'immutable' flag.
				return new BlindSearch(table, Column);
			}

			protected override int SearchFirst(TObject val) {
				if (val.IsNull) {
					return -1;
				}

				int num = ((BigNumber)val.Object).ToInt32();

				if (num < 0)
					return -1;
				if (num > (1 << BitCount))
					return -(((1 << BitCount)*BitCount) + 1);

				return (num * BitCount);
			}

			protected override int SearchLast(TObject val) {
				int p = SearchFirst(val);
				if (p >= 0)
					return p + (BitCount - 1);
				return p;
			}
		}
	}
}