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

using Deveel.Math;

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
		private const int BIT_COUNT = Privileges.BitCount;


		public GTPrivMapDataSource(DatabaseConnection connection)
			: base(connection.System) {
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return (1 << BIT_COUNT)*BIT_COUNT; }
		}

		public override TObject GetCellContents(int column, int row) {
			int c1 = row / BIT_COUNT;
			if (column == 0) {
				return GetColumnValue(column, (BigNumber)c1);
			} else {
				int priv_bit = (1 << (row % BIT_COUNT));
				String priv_string = null;
				if ((c1 & priv_bit) != 0) {
					priv_string = Privileges.FormatPriv(priv_bit);
				}
				return GetColumnValue(column, priv_string);
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		public override SelectableScheme GetColumnScheme(int column) {
			if (column == 0) {
				return new PrivMapSearch(this, column);
			} else {
				return new BlindSearch(this, column);
			}
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTPrivMapDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRPrivMap");

			// Add column definitions
			def.AddColumn(GetNumericColumn("priv_bit"));
			def.AddColumn(GetStringColumn("description"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;

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

				if (num < 0) {
					return -1;
				} else if (num > (1 << BIT_COUNT)) {
					return -(((1 << BIT_COUNT) * BIT_COUNT) + 1);
				}

				return (num * BIT_COUNT);
			}

			protected override int SearchLast(TObject val) {
				int p = SearchFirst(val);
				if (p >= 0) {
					return p + (BIT_COUNT - 1);
				} else {
					return p;
				}
			}
		}
	}
}