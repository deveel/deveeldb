using System;

namespace System.Runtime.Serialization.Formatters {
	[Serializable]
	public enum FormatterTypeStyle {
		TypesWhenNeeded = 0,
		TypesAlways = 1,
		XsdString = 2,
	}
}
