using System;

namespace Deveel.Data.Sql {
	public interface ISqlFormattable {
		void AppendTo(SqlStringBuilder builder);
	}
}
