using System;
using System.Text;

namespace Deveel.Data.Entity {
	internal abstract class BranchExpression : Expression {
		public BranchExpression Left { get; set; }
		public BranchExpression Right { get; set; }
		public string Name { get; set; }

		public bool Scoped { get; private set; }

		public virtual Expression GetProperty(string propertyName) {
			if (Left != null && Left.Name == propertyName) 
				return Left;
			if (Right != null && Right.Name == propertyName) 
				return Right;
			return null;
		}

		internal virtual void InsertInScope(ExpressionScope scope) {
			Scoped = true;

			if (scope != null) {
				if (Left != null)
					scope.Remove(Left);
				if (Right != null)
					scope.Remove(Right);
			}
		}

		protected virtual void InnerWrite(StringBuilder sb) {
		}

		internal override void WriteTo(StringBuilder sb) {
			if (Scoped)
				sb.Append('(');
			InnerWrite(sb);
			if (Scoped)
				sb.Append(')');

			if (Name != null) {
				if (this is TableExpression) {
					sb.Append(' ');
					sb.Append("AS");
					sb.Append(' ');
					sb.Append(Quote(Name));
				}
			}
		}
	}
}