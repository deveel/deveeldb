using System;
using System.Collections;
using System.Text;

namespace Deveel.Data.Entity {
	internal abstract class Expression {
		internal static string Quote(string id) {
			return "\"" + id + "\"";
		}

		internal abstract void WriteTo(StringBuilder sb);

		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			WriteTo(sb);
			return sb.ToString();
		}

		internal static void WriteList(IList list, StringBuilder sb) {
			for (int i = 0; i < list.Count; i++) {
				Expression expression = (Expression) list[i];
				sb.AppendLine();

				expression.WriteTo(sb);

				if (i < list.Count - 1)
					sb.Append(", ");
			}
		}
	}
}