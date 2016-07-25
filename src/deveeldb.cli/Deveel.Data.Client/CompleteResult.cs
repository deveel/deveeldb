using System;

namespace Deveel.Data.Client {
	public sealed class CompleteResult {
		private CompleteResult(string[] suggestions, string[] errors, bool valid) {
			Suggestions = suggestions;
			Errors = errors;
			IsValid = valid;
		}

		public string[] Suggestions { get; private set; }

		public string[] Errors { get; private set; }

		public bool IsValid { get; private set; }

		public static CompleteResult Suggest(string[] suggestions) {
			return new CompleteResult(suggestions, new string[0], true);
		}

		public static CompleteResult Fail(string[] errors) {
			return new CompleteResult(new string[0], errors, false);
		}
	}
}
