using System;

namespace Deveel.Data.Mapping {
	public interface ITypeMappingContext {
		INamingConvention TableNamingConvention { get; }

		INamingConvention ColumNamingConvention { get; }

		TypeMapping GetMapping(Type type);
	}
}
