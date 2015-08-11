using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class FetchContext {
		private int offset;

		public FetchContext(IQueryContext queryContext, SqlExpression reference) 
			: this(queryContext, FetchDirection.Next, reference) {
		}

		public FetchContext(IQueryContext queryContext, FetchDirection direction, SqlExpression reference) {
			if (queryContext == null)
				throw new ArgumentNullException("queryContext");
			if (reference == null)
				throw new ArgumentNullException("reference");

			if (reference.ExpressionType != SqlExpressionType.VariableReference &&
				reference.ExpressionType != SqlExpressionType.Reference)
				throw new ArgumentException("Invalid reference expression type.");

			QueryContext = queryContext;
			Direction = direction;
			Reference = reference;
		}

		public FetchDirection Direction { get; private set; }

		public SqlExpression Reference { get; set; }

		public bool IsVariableReference {
			get { return Reference.ExpressionType == SqlExpressionType.VariableReference; }
		}

		public bool IsGlobalReference {
			get { return Reference.ExpressionType == SqlExpressionType.Reference; }
		}

		public IQueryContext QueryContext { get; private set; }

		public int Offset {
			get { return offset; }
			set {
				if (Direction != FetchDirection.Absolute &&
					Direction != FetchDirection.Relative)
					throw new ArgumentException("Cannot set offset for this direction.");

				offset = value;
			}
		}
	}
}
