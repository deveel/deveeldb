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
using System.Collections.Generic;

using Deveel.Data.Deveel.Data.DbSystem;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Data.Util;

namespace Deveel.Data.DbSystem {
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
		private List<string> keyValuePairs;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DataTableInfo;

		static GTProductDataSource() {

			DataTableInfo info = new DataTableInfo(SystemSchema.ProductInfo);

			// Add column definitions
			info.AddColumn("var", PrimitiveTypes.VarString);
			info.AddColumn("value", PrimitiveTypes.VarString);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;

		}

		public GTProductDataSource(SimpleTransaction transaction)
			: base(transaction.Context) {
			keyValuePairs = new List<string>();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTProductDataSource Init() {
			ProductInfo productInfo = ProductInfo.Current;
			// Set up the product variables.
			keyValuePairs.Add("title");
			keyValuePairs.Add(productInfo.Title);

			keyValuePairs.Add("version");
			keyValuePairs.Add(productInfo.Version.ToString());

			keyValuePairs.Add("copyright");
			keyValuePairs.Add(productInfo.Copyright);

			keyValuePairs.Add("description");
			keyValuePairs.Add(productInfo.Description);

			keyValuePairs.Add("company");
			keyValuePairs.Add(productInfo.Company);

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return keyValuePairs.Count/2; }
		}

		public override TObject GetCell(int column, int row) {
			switch (column) {
				case 0:  // var
					return GetColumnValue(column, keyValuePairs[row * 2]);
				case 1:  // value
					return GetColumnValue(column, keyValuePairs[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			keyValuePairs = null;
		}
	}
}