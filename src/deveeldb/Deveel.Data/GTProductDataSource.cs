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
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that models 
	/// information about the software.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. It must not last
	/// beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTProductDataSource : GTDataSource {
		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private ArrayList key_value_pairs;

		public GTProductDataSource(SimpleTransaction transaction)
			: base(transaction.System) {
			key_value_pairs = new ArrayList();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTProductDataSource Init() {
			ProductInfo productInfo = ProductInfo.Current;
			// Set up the product variables.
			key_value_pairs.Add("title");
			key_value_pairs.Add(productInfo.Title);

			key_value_pairs.Add("version");
			key_value_pairs.Add(productInfo.Version.ToString());

			key_value_pairs.Add("copyright");
			key_value_pairs.Add(productInfo.Copyright);

			key_value_pairs.Add("description");
			key_value_pairs.Add(productInfo.Description);

			key_value_pairs.Add("company");
			key_value_pairs.Add(productInfo.Company);

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableDef TableInfo {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return key_value_pairs.Count/2; }
		}

		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // var
					return GetColumnValue(column, key_value_pairs[row * 2]);
				case 1:  // value
					return GetColumnValue(column, key_value_pairs[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			key_value_pairs = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTProductDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRProductInfo");

			// Add column definitions
			def.AddColumn(GetStringColumn("var"));
			def.AddColumn(GetStringColumn("value"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;

		}

	}
}