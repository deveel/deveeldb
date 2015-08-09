using System;

namespace Deveel.Data.Mapping {
	public interface INamingConvention {
		string FormatName(string inputName);
	}
}
