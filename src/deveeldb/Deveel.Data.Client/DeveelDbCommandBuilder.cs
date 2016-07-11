// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Client {
	public sealed class DeveelDbCommandBuilder : DbCommandBuilder {
		public DeveelDbCommandBuilder() 
			: this(null) {
		}

		public DeveelDbCommandBuilder(DeveelDbDataAdapter dataAdapter) {
			QuotePrefix = "'";
			QuoteSuffix = "'";
			DataAdapter = dataAdapter;
		}

		public new DeveelDbDataAdapter DataAdapter {
			get { return (DeveelDbDataAdapter) base.DataAdapter; }
			set { base.DataAdapter = value; }
		}

		[Browsable(false)]
		[DefaultValue("'")]
		public override string QuotePrefix {
			get { return base.QuotePrefix; }
			set { base.QuotePrefix = value; }
		}

		[Browsable(false)]
		[DefaultValue("'")]
		public override string QuoteSuffix {
			get { return base.QuoteSuffix; }
			set { base.QuoteSuffix = value; }
		}

		protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause) {
			var param = (DeveelDbParameter) parameter;
			param.SqlType = (SqlTypeCode)((int) row[SchemaTableColumn.ProviderType]);

			var size = (int?) row[SchemaTableColumn.ColumnSize];
			var scale = (int?) row[SchemaTableColumn.NumericScale];
			var nullable = (bool?) row[SchemaTableColumn.AllowDBNull];

			if (size != null)
				param.Size = size.Value;
			if (scale != null)
				param.Scale = (byte) scale.Value;
			if (nullable != null)
				param.IsNullable = nullable.Value;

			// TODO: apply any other setups?
		}

		protected override string GetParameterName(int parameterOrdinal) {
			return QueryParameter.Marker;
		}

		protected override string GetParameterName(string parameterName) {
			return String.Format("{0}{1}", QueryParameter.NamePrefix, parameterName);
		}

		protected override string GetParameterPlaceholder(int parameterOrdinal) {
			return String.Format("{0}p{1}", QueryParameter.NamePrefix, parameterOrdinal);
		}

		protected override void SetRowUpdatingHandler(DbDataAdapter adapter) {
			if (adapter == base.DataAdapter) {
				((DeveelDbDataAdapter) adapter).RowUpdating += OnRowUpdating;
			} else {
				((DeveelDbDataAdapter) adapter).RowUpdating -= OnRowUpdating;
			}
		}

		private void OnRowUpdating(object sender, RowUpdatingEventArgs e) {
			RowUpdatingHandler(e);
		}

		protected override DataTable GetSchemaTable(DbCommand sourceCommand) {
			using (var reader = sourceCommand.ExecuteReader(CommandBehavior.KeyInfo | CommandBehavior.SchemaOnly)) {
				var table = reader.GetSchemaTable();

				if (HasPrimaryKey(table))
					ResetIsUniqueColumn(table);

				return table;
			}
		}

		private bool HasPrimaryKey(DataTable table) {
			var column = table.Columns[SchemaTableColumn.IsKey];
			if (column == null)
				return false;

			foreach (DataRow schemaRow in table.Rows) {
				if ((bool)schemaRow[column])
					return true;
			}

			return false;
		}

		private void ResetIsUniqueColumn(DataTable schema) {
			var uniqueColumn = schema.Columns[SchemaTableColumn.IsUnique];
			var keyColumn = schema.Columns[SchemaTableColumn.IsKey];

			foreach (DataRow schemaRow in schema.Rows) {
				if ((bool)schemaRow[keyColumn])
					schemaRow[uniqueColumn] = false;
			}

			schema.AcceptChanges();
		}

		public new DeveelDbCommand GetDeleteCommand() {
			return (DeveelDbCommand) base.GetDeleteCommand();
		}

		public new DeveelDbCommand GetDeleteCommand(bool useColumnsForParameterNames) {
			return (DeveelDbCommand) base.GetDeleteCommand(useColumnsForParameterNames);
		}

		public new DeveelDbCommand GetUpdateCommand() {
			return (DeveelDbCommand) base.GetUpdateCommand();
		}

		public new DeveelDbCommand GetUpdateCommand(bool useColumnsForParameterNames) {
			return (DeveelDbCommand) base.GetUpdateCommand(useColumnsForParameterNames);
		}

		public new DeveelDbCommand GetInsertCommand() {
			return (DeveelDbCommand) base.GetInsertCommand();
		}

		public new DeveelDbCommand GetInsertCommand(bool useColumnsForParameterNames) {
			return (DeveelDbCommand) base.GetInsertCommand(useColumnsForParameterNames);
		}
	}
}
