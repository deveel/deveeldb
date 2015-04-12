using System;

namespace Deveel.Data.Types {
	public sealed class TabularType : DataType {
		public TabularType()
			: base("TABLE", SqlTypeCode.QueryPlanNode) {
		}
	}
}
