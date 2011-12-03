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