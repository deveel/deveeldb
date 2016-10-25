using System;

namespace Deveel.Data.Sql.Statements.Build {
	public interface ISelectLimitBuilder {
		ISelectLimitBuilder Count(long value);

		ISelectLimitBuilder Offset(long value);
	}
}
