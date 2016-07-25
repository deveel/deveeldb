using System;
using System.Collections.Generic;

namespace Deveel.Data.Client {
	public interface IOptionRegistry {
		IEnumerable<string> Keys { get; }

		void RegisterOption(IOption option);

		void RemoveOption(string key);

		IOption ResolveOption(string key);

		IOption UpdateOption(IOption option);
	}
}
