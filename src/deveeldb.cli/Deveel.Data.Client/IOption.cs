using System;

namespace Deveel.Data.Client {
	public interface IOption {
		string Key { get; }

		object Value { get; }

		object DefaultValue { get; }
	}
}
