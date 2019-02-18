using System;

namespace Deveel.Data.Sql.Statements {
	public static class StatementResultExtensions {
		public static bool IsEmpty(this IStatementResult result) {
			return result is EmptyStatementResult;
		}

		public static bool IsScalar(this IStatementResult result) {
			return result is StatementScalarResult;
		}

		public static SqlObject ScalarValue(this IStatementResult result) {
			if (!(result is StatementScalarResult))
				throw new InvalidOperationException("The result is not Scalar");

			return ((StatementScalarResult) result).Value;
		}
	}
}