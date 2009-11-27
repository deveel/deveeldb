//  
//  GTCurrentConnectionsDataSource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	internal class GTVariablesDataSource : GTDataSource {
		public GTVariablesDataSource(SimpleTransaction transaction) 
			: base(transaction.System) {
			this.transaction = this.transaction;
			key_value_pairs = new ArrayList();
		}

		static GTVariablesDataSource() {
			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRVariables");

			// Add column definitions
			def.AddColumn(GetStringColumn("var"));
			def.AddColumn(GetStringColumn("type"));
			def.AddColumn(GetStringColumn("value"));
			def.AddColumn(GetBooleanColumn("constant"));
			def.AddColumn(GetBooleanColumn("not_null"));
			def.AddColumn(GetBooleanColumn("is_set"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;
		}

		private SimpleTransaction transaction;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private ArrayList key_value_pairs;

		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return key_value_pairs.Count/4; }
		}

		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // var
					return GetColumnValue(column, key_value_pairs[row * 6]);
				case 1:  // type
					return GetColumnValue(column, key_value_pairs[(row * 6) + 1]);
				case 2:  // value
					return GetColumnValue(column, key_value_pairs[(row * 6) + 2]);
				case 3:  // constant
					return GetColumnValue(column, (bool)key_value_pairs[(row * 6) + 3]);
				case 4:  // not_null
					return GetColumnValue(column, (bool) key_value_pairs[(row * 6) + 4]);
				case 5:  // is_set
					return GetColumnValue(column, (bool)key_value_pairs[(row * 6) + 5]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		public GTVariablesDataSource Init() {
			VariablesManager variables = transaction.Variables;
			lock(variables) {
				for (int i = 0; i < variables.Count; i++) {
					Variable variable = variables[i];
					key_value_pairs.Add(variable.Name);
					key_value_pairs.Add(variable.Type.ToSQLString());
					key_value_pairs.Add(variable.IsSet ? variable.original_expression.Text.ToString() : "NULL");
					key_value_pairs.Add(variable.Constant);
					key_value_pairs.Add(variable.NotNull);
					key_value_pairs.Add(variable.IsSet);
				}

				return this;
			}
		}

		protected override void Dispose() {
			base.Dispose();
			key_value_pairs = null;
			transaction = null;
		}
	}
}