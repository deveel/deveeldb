using System;

namespace System {
	[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
	public sealed class SerializableAttribute : Attribute {
	}
}
