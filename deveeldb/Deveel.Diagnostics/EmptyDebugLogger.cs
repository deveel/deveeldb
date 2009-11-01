using System;

using Deveel.Data.Control;

namespace Deveel.Diagnostics {
	internal class EmptyDebugLogger : IDebugLogger {
		public void Dispose() {
		}

		public void Init(IDbConfig config) {
		}

		public bool IsInterestedIn(DebugLevel level) {
			return false;
		}

		public void Write(DebugLevel level, object ob, string message) {
		}

		public void Write(DebugLevel level, Type type, string message) {
		}

		public void Write(DebugLevel level, string typeString, string message) {
		}

		public void WriteException(Exception e) {
		}

		public void WriteException(DebugLevel level, Exception e) {
		}
	}
}