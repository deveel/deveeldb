using System;

namespace Deveel.Data.Client {
	public sealed class CompleteRequest {
		public CompleteRequest(string lineInput, string[] tokens, int currentTokenOffset) {
			LineInput = lineInput;
			Tokens = tokens;
			CurrentTokenOffset = currentTokenOffset;
		}

		public string LineInput { get; private set; }

		public string[] Tokens { get; private set; }

		public int CurrentTokenOffset { get; private set; }

		public string CurrentToken {
			get { return IsEmpty ? null : Tokens[CurrentTokenOffset]; }
		}

		public bool IsEmpty {
			get { return Tokens == null || Tokens.Length == 0; }
		}
	}
}
