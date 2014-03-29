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

using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="tableName">Name of the table to check the constraints.</param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we 
		/// need to determine if any constraint violations occurred.
		/// <para>
		/// To the constraint checking system, this is like adding all the 
		/// rows to the given table.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="tableName"/> was found.
		/// </exception>
		public void CheckAllConstraints(TableName tableName) {
			// Assert
			CheckExclusive();
			Transaction.CheckAllConstraints(tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.AddUniqueConstraint(DataConstraintInfo)"/>
		public void AddUniqueConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddUniqueConstraint(tableName, columns, deferred, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.AddForeignKeyConstraint(DataConstraintInfo)"/>
		public void AddForeignKeyConstraint(TableName table, string[] columns,
			TableName refTable, string[] refColumns,
			ConstraintAction deleteRule, ConstraintAction updateRule,
			ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddForeignKeyConstraint(table, columns, refTable, refColumns,
												deleteRule, updateRule,
												deferred, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.AddPrimaryKeyConstraint(DataConstraintInfo)"/>
		public void AddPrimaryKeyConstraint(TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddPrimaryKeyConstraint(tableName, columns, deferred, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.AddCheckConstraint(DataConstraintInfo)"/>
		public void AddCheckConstraint(TableName tableName, Expression expression, ConstraintDeferrability deferred, String constraintName) {
			// Assert
			CheckExclusive();
			Transaction.AddCheckConstraint(tableName, expression, deferred, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.DropAllConstraintsForTable"/>
		public void DropAllConstraintsForTable(TableName tableName) {
			// Assert
			CheckExclusive();
			Transaction.DropAllConstraintsForTable(tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.DropNamedConstraint"/>
		public int DropNamedConstraint(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return Transaction.DropNamedConstraint(tableName, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.DropPrimaryKeyConstraintForTable"/>
		public bool DropPrimaryKeyConstraintForTable(TableName tableName, string constraintName) {
			// Assert
			CheckExclusive();
			return Transaction.DropPrimaryKeyConstraintForTable(tableName, constraintName);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTablesRelationallyLinkedTo"/>
		public TableName[] QueryTablesRelationallyLinkedTo(TableName table) {
			return Transaction.QueryTablesRelationallyLinkedTo(Transaction, table);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTableUniques"/>
		public DataConstraintInfo[] QueryTableUniqueGroups(TableName tableName) {
			return Transaction.QueryTableUniques(Transaction, tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTablePrimaryKey"/>
		public DataConstraintInfo QueryTablePrimaryKeyGroup(TableName tableName) {
			return Transaction.QueryTablePrimaryKey(Transaction, tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTableCheckExpressions"/>
		public DataConstraintInfo[] QueryTableCheckExpressions(TableName tableName) {
			return Transaction.QueryTableCheckExpressions(Transaction, tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTableForeignKeys"/>
		public DataConstraintInfo[] QueryTableForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableForeignKeys(Transaction, tableName);
		}

		/// <inheritdoc cref="Transactions.Transaction.QueryTableImportedForeignKeys"/>
		public DataConstraintInfo[] QueryTableImportedForeignKeyReferences(TableName tableName) {
			return Transaction.QueryTableImportedForeignKeys(Transaction, tableName);
		}
	}
}