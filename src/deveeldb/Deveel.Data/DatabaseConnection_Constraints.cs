// 
//  Copyright 2010-2011  Deveel
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

namespace Deveel.Data {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="table_name">Name of the table to check the constraints.</param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we 
		/// need to determine if any constraint violations occurred.
		/// <para>
		/// To the constraint checking system, this is like adding all the 
		/// rows to the given table.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="table_name"/> was found.
		/// </exception>
		public void CheckAllConstraints(TableName table_name) {
			// Assert
			CheckExclusive();
			Transaction.CheckAllConstraints(table_name);
		}

		/// <inheritdoc cref="Data.Transaction.AddUniqueConstraint"/>
		public void AddUniqueConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddUniqueConstraint(tableName, columns, deferred, constraintName);
		}

		/// <inheritdoc cref="Data.Transaction.AddForeignKeyConstraint"/>
		public void AddForeignKeyConstraint(TableName table, string[] columns,
			TableName ref_table, string[] refColumns,
			ConstraintAction delete_rule, ConstraintAction update_rule,
			ConstraintDeferrability deferred, String constraint_name) {
			// Assert
			CheckExclusive();
			Transaction.AddForeignKeyConstraint(table, columns, ref_table, refColumns,
												delete_rule, update_rule,
												deferred, constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.AddPrimaryKeyConstraint"/>
		public void AddPrimaryKeyConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, String constraint_name) {
			// Assert
			CheckExclusive();
			Transaction.AddPrimaryKeyConstraint(tableName, columns, deferred, constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.AddCheckConstraint"/>
		public void AddCheckConstraint(TableName tableName, Expression expression, ConstraintDeferrability deferred, String constraint_name) {
			// Assert
			CheckExclusive();
			Transaction.AddCheckConstraint(tableName, expression, deferred, constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.DropAllConstraintsForTable"/>
		public void DropAllConstraintsForTable(TableName tableName) {
			// Assert
			CheckExclusive();
			Transaction.DropAllConstraintsForTable(tableName);
		}

		/// <inheritdoc cref="Data.Transaction.DropNamedConstraint"/>
		public int DropNamedConstraint(TableName table_name, String constraint_name) {
			// Assert
			CheckExclusive();
			return Transaction.DropNamedConstraint(table_name, constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.DropPrimaryKeyConstraintForTable"/>
		public bool DropPrimaryKeyConstraintForTable(TableName table_name, String constraint_name) {
			// Assert
			CheckExclusive();
			return Transaction.DropPrimaryKeyConstraintForTable(table_name,
																	 constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTablesRelationallyLinkedTo"/>
		public TableName[] QueryTablesRelationallyLinkedTo(TableName table) {
			return Transaction.QueryTablesRelationallyLinkedTo(Transaction, table);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableUniqueGroups"/>
		public Transaction.ColumnGroup[] QueryTableUniqueGroups(TableName table_name) {
			return Transaction.QueryTableUniqueGroups(Transaction, table_name);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTablePrimaryKeyGroup"/>
		public Transaction.ColumnGroup QueryTablePrimaryKeyGroup(TableName table_name) {
			return Transaction.QueryTablePrimaryKeyGroup(Transaction, table_name);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableCheckExpressions"/>
		public Transaction.CheckExpression[] QueryTableCheckExpressions(TableName table_name) {
			return Transaction.QueryTableCheckExpressions(Transaction, table_name);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableForeignKeyReferences"/>
		public Transaction.ColumnGroupReference[] QueryTableForeignKeyReferences(TableName table_name) {
			return Transaction.QueryTableForeignKeyReferences(Transaction, table_name);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableImportedForeignKeyReferences"/>
		public Transaction.ColumnGroupReference[] QueryTableImportedForeignKeyReferences(TableName table_name) {
			return Transaction.QueryTableImportedForeignKeyReferences(Transaction, table_name);
		}
	}
}