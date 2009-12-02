//  
//  IntoClause.cs
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

namespace Deveel.Data.Sql {
	public sealed class IntoClause {
		public IntoClause() {
			elements = new ArrayList();
		}

		private string tableName;

		private TableName resolvedTableName;

		/// <summary>
		/// The list of target elements (VariableRef
		/// or TableName).
		/// </summary>
		private readonly ArrayList elements;

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
			if (tableName !=null && tableName.Length > 0)
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
					DataTableColumnDef srcColumnDef = table.DataTableDef[i];
					DataTableColumnDef dstColumnDef = dest_table.DataTableDef[i];

					if (srcColumnDef.TType != dstColumnDef.TType)
						throw new DatabaseException("The column " + dstColumnDef.Name + " in the destination table has a different " +
													"type from the source column " + srcColumnDef.Name);
				}

				int rowCount = table.RowCount;
				for (int i = 0; i < rowCount; i++) {
					RowData row = new RowData(dest_table);

					for (int j = 0; j < colCount; j++) {
						TObject cell = table.GetCellContents(j, i);
						row.SetColumnDataFromTObject(j, cell);
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

					DataTableColumnDef columnDef = table.DataTableDef[i];
					TObject cell = table.GetCellContents(i, 0);

					if (columnDef.TType.SQLType != variable.Type.SQLType) {
						try {
							cell = cell.CastTo(variable.Type);
						} catch {
							throw new DatabaseException("The destination variable " + variable.Name + " has a different type than " +
							"the source column " + columnDef.Name + " and it's not possible to convert it.");
						}
					}

					variable.SetValue(cell);
				}

				return FunctionTable.ResultTable(context, 1);
			}
		}
	}
}