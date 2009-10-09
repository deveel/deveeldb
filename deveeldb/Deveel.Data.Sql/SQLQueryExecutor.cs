using System;
using System.Data;
using System.IO;

using Deveel.Data.Client;

namespace Deveel.Data.Sql {
	///<summary>
	/// An object used to execute SQL queries against a given <see cref="DatabaseConnection"/> 
	/// object.
	///</summary>
	/// <remarks>
	/// The object maintains an SQL parser object as state which is reused 
	/// as necessary.
	/// <para>
	/// This object is a convenient way to execute SQL queries.
	/// </para>
	/// </remarks>
	public class SQLQueryExecutor {
		/// <summary>
		/// The SQL parser state.
		/// </summary>
		private readonly static SQL sql_parser;

		static SQLQueryExecutor() {
			// Set up the sql parser.
			sql_parser = new SQL(new StringReader(""));
		}

		///<summary>
		/// Executes the given <see cref="SqlCommand"/> object on the given 
		/// <see cref="DatabaseConnection"/> object.
		///</summary>
		///<param name="connection"></param>
		///<param name="command"></param>
		/// <remarks>
		/// This method does not perform any locking. Any locking must have happened 
		/// before this method is called.
		/// <para>
		/// Also the returned <see cref="Table"/> object is onld valid within the
		/// life-time of the lock unless the root lock requirements are satisified.
		/// </para>
		/// </remarks>
		///<returns>
		/// Returns a <see cref="Table"/> object that contains the result of the execution.
		/// </returns>
		///<exception cref="DataException"></exception>
		public Table Execute(DatabaseConnection connection, SqlCommand command) {

			// StatementTree caching

			// Create a new parser and set the parameters...
			String query_str = command.Query;
			StatementTree statement_tree = null;
			StatementCache statement_cache =
										  connection.System.StatementCache;

			if (statement_cache != null) {
				// Is this command cached?
				statement_tree = statement_cache.Get(query_str);
			}
			if (statement_tree == null) {
				lock (sql_parser) {
					sql_parser.ReInit(new StringReader(query_str));
					sql_parser.Reset();
					// Parse the statement.
					statement_tree = sql_parser.Statement();
				}
				// Put the statement tree in the cache
				if (statement_cache != null) {
					statement_cache.Set(query_str, statement_tree);
				}
			}

			// Substitute all parameter substitutions in the statement tree.
			Object[] vars = command.Variables;
			IExpressionPreparer preparer = new ExpressionPreparerImpl(vars);
			statement_tree.PrepareAllExpressions(preparer);

			// Convert the StatementTree to a statement object
			Statement statement;
			Type statement_class = statement_tree.StatementType;
			try {
				statement = (Statement)Activator.CreateInstance(statement_class);
			} catch (TypeLoadException) {
				throw new DataException(
							  "Could not find statement class: " + statement_class);
			} catch (TypeInitializationException e) {
				throw new DataException(
							  "Could not instantiate class: " + statement_class);
			} catch (AccessViolationException e) {
				throw new DataException(
							  "Could not access class: " + statement_class);
			}


			// Initialize the statement
			statement.Init(connection, statement_tree, command);

			// Automated statement tree preparation
			statement.ResolveTree();

			// Prepare the statement.
			statement.Prepare();

			// Evaluate the SQL statement.
			Table result = statement.Evaluate();

			return result;

		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			private Object[] vars;

			public ExpressionPreparerImpl(object[] vars) {
				this.vars = vars;
			}

			public bool CanPrepare(Object element) {
				return (element is ParameterSubstitution);
			}
			public Object Prepare(Object element) {
				ParameterSubstitution ps = (ParameterSubstitution)element;
				int param_id = ps.Id;
				return TObject.GetObject(vars[param_id]);
			}
		}
	}
}