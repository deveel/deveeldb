using System;

namespace Deveel.Data.Sql.Statements {
	public static class StatementResultExtensions {
		public static bool IsEmpty(this IStatementResult result) {
			return result is EmptyStatementResult;
		}

		public static bool IsScalar(this IStatementResult result) {
			return result is StatementScalarResult;
		}

		public static bool IsError(this IStatementResult result) {
			return result is StatementErrorResult;
		}

		public static SqlStatementException Error(this IStatementResult result) {
			if (!(result is StatementErrorResult))
				throw new InvalidOperationException("The result is not an Error");

			return ((StatementErrorResult) result).Error;
		}

		public static SqlObject ScalarValue(this IStatementResult result) {
			if (!(result is StatementScalarResult))
				throw new InvalidOperationException("The result is not Scalar");

			return ((StatementScalarResult) result).Value;
		}
	}
}