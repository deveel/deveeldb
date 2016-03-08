using System;

namespace Deveel.Data.Sql.Expressions {
	public interface IExpressionParser {
		SqlExpression Parse(string s);
	}
}
