using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	public class FunctionExpression : QueryExpression {
		public FunctionExpression(Type type, string functionName, IEnumerable<Expression> arguments)
			: base(QueryExpressionType.Function, type) {
			FunctionName = functionName;
			Arguments = arguments.ToReadOnly();
		}

		public string FunctionName { get; private set; }

		public ReadOnlyCollection<Expression> Arguments { get; private set; }
	}
}
