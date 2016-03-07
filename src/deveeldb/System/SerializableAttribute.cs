using System;

namespace System {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Enum)]
	public sealed class SerializableAttribute : Attribute {
	}
}
