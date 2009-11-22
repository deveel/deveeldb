using System;

namespace Deveel.Data.Select {
	public sealed class JoinPart {
		private readonly JoinType type;
		private readonly string onExpression;

		public JoinPart(JoinType type, string onExpression) {
			this.type = type;
			this.onExpression = onExpression;
		}

		public JoinPart(JoinType type)
			: this(type, null) {
		}

		public string OnExpression {
			get { return onExpression; }
		}

		public JoinType Type {
			get { return type; }
		}
	}
}