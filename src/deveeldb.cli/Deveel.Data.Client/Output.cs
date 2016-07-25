using System;

namespace Deveel.Data.Client {
	public static class Output {
		public static readonly IOutputTarget Null = new NullOutputTarget();

		private static IOutputTarget output;
		private static IOutputTarget error;

		static Output() {
			Current = new NullOutputTarget();
		}

		public static IOutputTarget Current {
			get { return output; }
			set {
				if (value == null)
					value = Null;
				output = value;
			}
		}

		public static IOutputTarget Error {
			get { return error; }
			set {
				if (value == null)
					value = Null;
				error = value;
			}
		}

		#region NullOutputTarget

		class NullOutputTarget : IOutputTarget {
			public void Dispose() {
			}

			public bool IsInteractive {
				get { return false; }
			}

			public void Write(char[] buffer, int offset, int length) {
			}

			public void Flush() {
			}

			public void SetAttribute(OutputAttributes attributes) {
			}

			public void Close() {
			}
		}

		#endregion
	}
}
