using System;

namespace Deveel.Data.Client {
	public interface IOptions {
		bool HasOption(string option);

		object GetValue(string option);
	}
}
