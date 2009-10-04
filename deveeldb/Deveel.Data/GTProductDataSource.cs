//  
//  GTProductDataSource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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

		public override DataTableDef DataTableDef {
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

		protected override void Dispose() {
			base.Dispose();
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