using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Class)]
	public sealed class CheckAttribute : Attribute {
		public CheckAttribute(string expression) {
			if (String.IsNullOrEmpty(expression))
				throw new ArgumentNullException("expression");

			Expression = expression;
		}
		public string Expression { get; private set; }
	}
}
