using System;

namespace Deveel.Data.Entity {
	internal abstract class NegatableExpression : Expression {
		protected bool Negated { get; private set; }

		public void Negate() {
			Negated = !Negated;
		}
	}
}