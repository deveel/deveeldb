// 
//  Copyright 2011 Deveel
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Exposes the context of a single <see cref="statement"/>, providing
	/// necessary features for the preparation and evaluation of
	/// the parent statement.
	/// </summary>
	public sealed class StatementContext {
		/// <summary>
		/// The statement this context refers to.
		/// </summary>
		private readonly Statement statement;

		/// <summary>
		/// The Database context.
		/// </summary>
		private DatabaseConnection connection;

		/// <summary>
		/// The StatementTree object that is the container for the query.
		/// </summary>
		private StatementTree tree;

		/// <summary>
		/// The tree object prior to a call to Reset
		/// </summary>
		private StatementTree originalTree;

		/// <summary>
		/// The SqlQuery object that was used to produce this statement.
		/// </summary>
		private SqlQuery query;

		internal StatementContext(Statement statement) {
			this.statement = statement;
			tree = new StatementTree(statement.GetType());
		}

		public DatabaseConnection Connection {
			get { return connection; }
		}

		public User User {
			get { return connection != null ? connection.User : null; }
		}

		internal StatementTree StatementTree {
			get { return tree; }
		}

		public SqlQuery Query {
			get { return query; }
		}

		/// <summary>
		/// Performs initial preparation on the contents of the 
		/// <see cref="StatementTree"/> by resolving all sub queries and 
		/// mapping functions to their executable forms.
		/// </summary>
		/// <remarks>
		/// Given a <see cref="StatementTree"/> and a <see cref="Database"/> 
		/// context, this method will convert all sub-queries found in 
		/// <see cref="StatementTree"/> to a <i>queriable</i> object. 
		/// In other words, all <see cref="StatementTree"/> are converted to 
		/// <see cref="SelectStatement"/>.
		/// </remarks>
		private void ResolveTree(DatabaseConnection c) {
			// For every expression in this select we must go through and resolve
			// any sub-queries we find to the correct Select object.
			// This method will prepare the sub-query substitute the StatementTree
			// object for a Select object in the expression.
			tree.PrepareAllExpressions(new StatementTreePreparer(c));
		}

		internal void Set(DatabaseConnection dbConnection, StatementTree statementTree, SqlQuery sqlQuery) {
			connection = dbConnection;
			query = sqlQuery;

			if (statementTree == null && originalTree != null) {
				tree = (StatementTree) originalTree.Clone();
			} else {
				originalTree = tree;
				tree = statementTree;
			}

			PrepareTree(connection);
		}

		internal void PrepareTree(DatabaseConnection c) {
			if (c != null)
				ResolveTree(c);

			// Prepare the statement.
			statement.PrepareStatement();
		}

		internal void Reset() {
			connection = null;
			query = null;

			if (originalTree != null)
				tree = (StatementTree) originalTree.Clone();

			statement.Reset();
		}

		private class StatementTreePreparer : IExpressionPreparer {
			private readonly DatabaseConnection connection;

			public StatementTreePreparer(DatabaseConnection connection) {
				this.connection = connection;
			}

			public bool CanPrepare(object element) {
				return element is StatementTree;
			}

			public object Prepare(object element) {
				StatementTree statementTree = (StatementTree)element;
				SelectStatement statement = new SelectStatement();
				statement.Context.Set(connection, statementTree, null);
				return statement;
			}
		}
	}
}