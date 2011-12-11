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

		/// <inheritdoc cref="Data.Transaction.AddUniqueConstraint(Deveel.Data.DataConstraintInfo)"/>
		public void AddUniqueConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddUniqueConstraint(tableName, columns, deferred, constraintName);
		}

		/// <inheritdoc cref="Data.Transaction.AddForeignKeyConstraint(Deveel.Data.DataConstraintInfo)"/>
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

		/// <inheritdoc cref="Data.Transaction.AddPrimaryKeyConstraint(Deveel.Data.DataConstraintInfo)"/>
		public void AddPrimaryKeyConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, String constraint_name) {
			// Assert
			CheckExclusive();
			Transaction.AddPrimaryKeyConstraint(tableName, columns, deferred, constraint_name);
		}

		/// <inheritdoc cref="Data.Transaction.AddCheckConstraint(Deveel.Data.DataConstraintInfo)"/>
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
		public int DropNamedConstraint(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return Transaction.DropNamedConstraint(tableName, constraintName);
		}

		/// <inheritdoc cref="Data.Transaction.DropPrimaryKeyConstraintForTable"/>
		public bool DropPrimaryKeyConstraintForTable(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return Transaction.DropPrimaryKeyConstraintForTable(tableName, constraintName);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTablesRelationallyLinkedTo"/>
		public TableName[] QueryTablesRelationallyLinkedTo(TableName table) {
			return Transaction.QueryTablesRelationallyLinkedTo(Transaction, table);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableUniques"/>
		public DataConstraintInfo[] QueryTableUniqueGroups(TableName tableName) {
			return Transaction.QueryTableUniques(Transaction, tableName);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTablePrimaryKey"/>
		public DataConstraintInfo QueryTablePrimaryKeyGroup(TableName tableName) {
			return Transaction.QueryTablePrimaryKey(Transaction, tableName);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableCheckExpressions"/>
		public DataConstraintInfo[] QueryTableCheckExpressions(TableName tableName) {
			return Transaction.QueryTableCheckExpressions(Transaction, tableName);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableForeignKeys"/>
		public DataConstraintInfo[] QueryTableForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableForeignKeys(Transaction, tableName);
		}

		/// <inheritdoc cref="Data.Transaction.QueryTableImportedForeignKeys"/>
		public DataConstraintInfo[] QueryTableImportedForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableImportedForeignKeys(Transaction, tableName);
		}
	}
}