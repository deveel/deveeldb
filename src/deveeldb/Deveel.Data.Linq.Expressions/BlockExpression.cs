using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	public class BlockExpression : QueryExpression {
		public BlockExpression(IEnumerable<Expression> expressions)
			: base(QueryExpressionType.Block, expressions.Last().Type) {
			Expressions = expressions.ToReadOnly();
		}

		public ReadOnlyCollection<Expression> Expressions { get; private set; }
	}
}
