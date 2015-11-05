using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public class PlSqlBlock : IPreparable, IDisposable {
		private ICollection<SqlStatement> statements;
		private ICollection<ExceptionHandler> exceptionHandlers; 
		 
		public PlSqlBlock() {
			statements = new List<SqlStatement>();
			exceptionHandlers = new List<ExceptionHandler>();
		}

		~PlSqlBlock() {
			Dispose(false);
		}

		public string Label { get; set; }

		public IEnumerable<SqlStatement> Statements {
			get { return statements.AsEnumerable(); }
		}

		public IEnumerable<ExceptionHandler> ExceptionHandlers {
			get { return exceptionHandlers.AsEnumerable(); }
		}

		public void AddStatement(SqlStatement statement) {
			// TODO: make further checks, such as if a labeled statement with
			//       the same label already exists
			statements.Add(statement);
		}

		public void AddExceptionHandler(ExceptionHandler handler) {
			// TODO: make further checks here ...
			exceptionHandlers.Add(handler);
		}

		protected virtual BlockExecuteContext CreateExecuteContext() {
			throw new NotImplementedException();
		}

		protected virtual PlSqlBlock Prepare(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return Prepare(preparer);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (statements != null)
					statements.Clear();
				if (exceptionHandlers != null)
					exceptionHandlers.Clear();
			}

			statements = null;
			exceptionHandlers = null;
		}
	}
}
