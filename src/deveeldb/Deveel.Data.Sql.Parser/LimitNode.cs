using System;

namespace Deveel.Data.Sql.Parser {
	class LimitNode : SqlNode {
		public long Count { get; private set; }

		public long Offset { get; private set; }

		protected override void OnNodeInit() {
			long? arg1 = null, arg2 = null;
			foreach (var childNode in ChildNodes) {
				if (childNode is IntegerLiteralNode) {
					if (arg1 == null) {
						arg1 = ((IntegerLiteralNode) childNode).Value;
					} else if (arg2 == null) {
						arg2 = ((IntegerLiteralNode) childNode).Value;
					}
				}
			}

			if (arg1 == null && arg2 == null)
				throw new SqlParseException("At least one parameter is required in a LIMIT.");

			if (arg2 != null) {
				Offset = arg1.Value;
				Count = arg2.Value;
			} else {
				Count = arg1.Value;
			}

			base.OnNodeInit();
		}
	}
}
