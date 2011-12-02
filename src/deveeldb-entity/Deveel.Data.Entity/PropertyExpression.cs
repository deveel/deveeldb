using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Entity {
	internal class PropertyExpression : Expression {
		public PropertyExpression() {
			Properties = new List<string>();
		}

		public List<string> Properties { get; private set; }

		internal override void WriteTo(StringBuilder sql) {
			throw new InvalidOperationException();
		}
	}
}