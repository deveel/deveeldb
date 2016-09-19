using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public interface IMemberConfiguration {
		IMemberConfiguration HasColumnName(string value);

		IMemberConfiguration HasColumnType(SqlType value);

		IMemberConfiguration NotNull(bool value = true);

		IMemberConfiguration PrimaryKey(bool value = true);

		IMemberConfiguration Unique(bool value = true);

		IMemberConfiguration HasDefault(SqlExpression defaultExpression);

		IMemberConfiguration Ignore(bool value = true);
	}
}
