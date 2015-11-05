using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	interface IFunctionQueryContext : IBlockQueryContext {
		void SetReturn(SqlExpression expression);
	}
}
