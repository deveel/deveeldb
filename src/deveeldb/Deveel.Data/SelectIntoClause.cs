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
	public sealed class SelectIntoClause : ICloneable {
		public SelectIntoClause() {
			elements = new ArrayList();
		}

		private string tableName;

		private TableName resolvedTableName;

		/// <summary>
		/// The list of target elements (VariableRef
		/// or TableName).
		/// </summary>
		private ArrayList elements;

		internal bool HasElements {
			get { return elements.Count > 0; }
		}

		public object this[int index] {
			get { return elements[index]; }
		}

		internal bool HasTableName {
			get { return elements[0] is string || elements[0] is TableName; }
		}

		/// <summary>
		/// If the clause defines a 
		/// </summary>
		public TableName Table {
			get { return resolvedTableName; }
		}

		internal void ResolveTableName(DatabaseConnection connection) {
			resolvedTableName = TableName.Resolve(connection.CurrentSchema, tableName);
		}

		internal void SetTableName(string value) {
			if (!string.IsNullOrEmpty(tableName))
				throw new ArgumentException("Cannot set more than one destination table.");

			tableName = value;
		}

		/// <summary>
		/// Adds an element as a target of the clause.
		/// </summary>
		/// <param name="value">The target element to add.</param>
		public void AddElement(object value) {
			if (value == null)
				throw new ArgumentNullException("value");

			if (value is TableName ||
				value is string ||
				value is VariableRef) {
				if ((value is string && tableName != null) ||
					(value is TableName && resolvedTableName != null))
					throw new ArgumentException("Cannot set more than one destination table.");

				if (value is string) {
					tableName = (string) value;
				} else if (value is TableName) {
					resolvedTableName = (TableName)value;
				} else {
					elements.Add(value);
				}
			} else {
				throw new ArgumentException("Unable to set the object as target of an INTO clause.");
			}
		}

		public Table SelectInto(DatabaseQueryContext context, Table table) {
			if (Table != null) {
				DataTable dest_table = context.Connection.GetTable(Table);
				if (dest_table == null)
					throw new DatabaseException("The table '" + Table + "' target of the select statement was not found.");

				int colCount = table.ColumnCount;
				if (colCount != dest_table.ColumnCount)
					throw new DatabaseException("The number of columns between source table and the destination " +
												"table is different.");

				for (int i = 0; i < colCount; i++) {
					DataTableColumnInfo srcColumnInfo = table.TableInfo[i];
					DataTableColumnInfo dstColumnInfo = dest_table.TableInfo[i];

					if (srcColumnInfo.TType != dstColumnInfo.TType)
						throw new DatabaseException("The column " + dstColumnInfo.Name + " in the destination table has a different " +
													"type from the source column " + srcColumnInfo.Name);
				}

				int rowCount = table.RowCount;
				for (int i = 0; i < rowCount; i++) {
					DataRow row = new DataRow(dest_table);

					for (int j = 0; j < colCount; j++) {
						TObject cell = table.GetCellContents(j, i);
						row.SetValue(j, cell);
					}

					dest_table.Add(row);
				}

				return FunctionTable.ResultTable(context, rowCount);
			} else {
				int colCount = table.ColumnCount;
				if (colCount != elements.Count)
					throw new DatabaseException("The number of columns between the source table and the destination " +
												"variables is differente");

				for (int i = 0; i < colCount; i++) {
					VariableRef varRef = (VariableRef)elements[i];
					Variable variable = context.GetVariable(varRef.Variable);
					if (variable == null)
						throw new DatabaseException("The destination variable " + varRef.Variable + " was not found.");

					DataTableColumnInfo columnInfo = table.TableInfo[i];
					TObject cell = table.GetCellContents(i, 0);

					if (columnInfo.TType.SQLType != variable.Type.SQLType) {
						try {
							cell = cell.CastTo(variable.Type);
						} catch {
							throw new DatabaseException("The destination variable " + variable.Name + " has a different type than " +
							"the source column " + columnInfo.Name + " and it's not possible to convert it.");
						}
					}

					variable.SetValue(cell);
				}

				return FunctionTable.ResultTable(context, 1);
			}
		}

		#region Implementation of ICloneable

		public object Clone() {
			SelectIntoClause clause = new SelectIntoClause();
			if (tableName != null)
				clause.tableName = tableName;
			if (resolvedTableName != null)
				clause.resolvedTableName = resolvedTableName;
			clause.elements = (ArrayList) elements.Clone();
			return clause;
		}

		#endregion
	}
}