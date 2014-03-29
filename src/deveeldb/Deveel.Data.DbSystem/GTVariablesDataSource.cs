// 
//  Copyright 2010-2013  Deveel
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
using System.Collections.Generic;

using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	internal class GTVariablesDataSource : GTDataSource {
		public GTVariablesDataSource(SimpleTransaction transaction) 
			: base(transaction.Context) {
			this.transaction = transaction;
			variables = new List<VariableInfo>();
		}

		static GTVariablesDataSource() {
			DataTableInfo info = new DataTableInfo(SystemSchema.Variables);

			// Add column definitions
			info.AddColumn("var", TType.StringType);
			info.AddColumn("type", TType.StringType);
			info.AddColumn("value", TType.StringType);
			info.AddColumn("constant", TType.BooleanType);
			info.AddColumn("not_null", TType.BooleanType);
			info.AddColumn("is_set", TType.BooleanType);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;
		}

		private SimpleTransaction transaction;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private List<VariableInfo> variables;

		internal static readonly DataTableInfo DataTableInfo;

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return variables.Count/4; }
		}

		public override TObject GetCell(int column, int row) {
			VariableInfo variable = variables[row];

			switch (column) {
				case 0:  // var
					return GetColumnValue(column, variable.Name);
				case 1:  // type
					return GetColumnValue(column, variable.SqlType);
				case 2:  // value
					return GetColumnValue(column, variable.Value);
				case 3:  // constant
					return GetColumnValue(column, variable.IsConstant);
				case 4:  // not_null
					return GetColumnValue(column, variable.IsNotNull);
				case 5:  // is_set
					return GetColumnValue(column, variable.IsSet);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		public GTVariablesDataSource Init() {
			VariablesManager variablesManager = transaction.Variables;
			lock(variablesManager) {
				for (int i = 0; i < variablesManager.Count; i++) {
					Variable variable = variablesManager[i];
					variables.Add(new VariableInfo(variable));
				}

				return this;
			}
		}

		protected override void Dispose(bool disposing) {
			variables = null;
			transaction = null;
		}

		#region DbVariable

		class VariableInfo {
			public VariableInfo(Variable variable) {
				Name = variable.Name;
				SqlType = variable.Type.ToSQLString();
				Value = variable.IsSet ? variable.original_expression.Text.ToString() : "NULL";
				IsConstant = variable.Constant;
				IsNotNull = variable.NotNull;
				IsSet = variable.IsSet;
			} 

			public readonly string Name;
			public readonly string SqlType;
			public readonly string Value;
			public readonly bool IsConstant;
			public readonly bool IsNotNull;
			public readonly bool IsSet;
		}

		#endregion
	}
}