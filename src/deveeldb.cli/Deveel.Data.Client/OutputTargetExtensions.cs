using System;

namespace Deveel.Data.Client {
	public static class OutputTargetExtensions {
		public static void Write(this IOutputTarget target, string text) {
			var chars = text.ToCharArray();
			target.Write(chars, 0, chars.Length);
		}

		public static void WriteLine(this IOutputTarget target, string line) {
			target.Write(line);
			target.WriteLine();
		}

		public static void WriteLine(this IOutputTarget target) {
			target.Write(Environment.NewLine);
		}

		public static void ResetAttributes(this IOutputTarget target) {
			target.SetAttribute(OutputAttributes.None);
		}
	}
}
